from abc import ABC, abstractmethod
from pathlib import Path
from shlex import quote as shell_escape


__all__ = ["ShellScript", "ShellCommand"]


class AbstractAction(ABC):
    def __init__(self):
        self.tracking_status_path = None

    @abstractmethod
    def to_command(self):
        raise NotImplementedError("Return command that executes the action.")

    def enable_tracking(self, status_path):
        self.tracking_status_path = Path(status_path)

    def get_status(self):
        tracking_status_path = getattr(self, "tracking_status_path", None)
        assert isinstance(tracking_status_path, Path)

        if not tracking_status_path.exists():
            return -2

        with open(tracking_status_path) as status_file:
            # limit number of bytes for better security
            exit_code = status_file.read(16)

            if len(exit_code) == 0:
                return -1
            else:
                return int(exit_code)

    def clean_up_tracking_status_file(self):
        tracking_status_path = getattr(self, "tracking_status_path", None)
        if isinstance(tracking_status_path, Path) and tracking_status_path.exists():
            tracking_status_path.unlink()

    def __del__(self):
        self.clean_up_tracking_status_file()

    def __str__(self):
        return " ".join(shell_escape(part) for part in self.to_command())


class ShellScript(AbstractAction):
    def __init__(
        self, *commands, shell=["/bin/bash", "-c"], safe_mode="set -euo pipefail"
    ):
        super().__init__()
        for command in commands:
            assert isinstance(command, ShellCommand)
        self.commands = commands
        self.shell = shell
        self.safe_mode = safe_mode

    def append(self, command):
        assert isinstance(command, ShellCommand)
        self.commands.append(command)

    def to_command(self):
        script = "; ".join(str(command) for command in self.commands)
        if self.safe_mode is not None:
            script = f"{self.safe_mode}; {script}"

        if self.tracking_status_path is not None:
            status_path = shell_escape(str(self.tracking_status_path))
            preface = f"touch {status_path}"
            epilogue = f"S=$?; echo $S > {status_path}; exit $S"
            script = f"{preface}; ( {script} ); {epilogue}"

        return [*self.shell, script]


class ShellCommand(object):
    def __init__(self, parts=[], stdin=None, stdout=None, stderr=None):
        self.parts = [shell_escape(str(part)) for part in parts]
        self.stdin(stdin)
        self.stdout(stdout)
        self.stderr(stderr)

    def append(self, part):
        self.parts.append(shell_escape(str(part)))

    def __add__(self, part):
        self.append(part)

    def stdin(self, file_path):
        self._stdin = self.__get_path(file_path)

    def stdout(self, file_path):
        self._stdout = self.__get_path(file_path)

    def stderr(self, file_path):
        self._stderr = self.__get_path(file_path)

    def __get_path(self, file_path):
        if file_path is None:
            return None
        else:
            return Path(file_path)

    def pipe(self, *new_parts):
        if len(new_parts) == 0 and isinstance(new_parts[0], ShellCommand):
            new_parts = new_parts[0].parts

        self.parts.append("|")
        for new_part in new_parts:
            self.append(new_part)

    def __or__(self, command):
        assert isinstance(command, ShellCommand)
        self.pipe(command)

    def __str__(self):
        all_parts = self.parts

        self.__append_redirection(all_parts, "_stdin", "<")
        self.__append_redirection(all_parts, "_stdout", ">")
        self.__append_redirection(all_parts, "_stderr", "2>")

        return " ".join(self.parts)

    def __append_redirection(self, all_parts, attr, redirection):
        file_path = getattr(self, attr)

        if file_path is not None:
            esc_file = shell_escape(str(file_path))
            all_parts.append(f"{redirection} {esc_file}")
