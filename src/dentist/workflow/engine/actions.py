from abc import ABC, abstractmethod
from pathlib import Path

try:
    from shlex import quote as shell_escape
except ImportError:
    from pipes import quote as shell_escape


__all__ = ["ShellScript", "ShellCommand"]


class AbstractAction(ABC):
    @abstractmethod
    def to_command(self):
        raise NotImplementedError("Return command that executes the action.")

    def __str__(self):
        return " ".join(shell_escape(part) for part in self.to_command())


class ShellScript(AbstractAction):
    def __init__(self, commands=[], shell=["/usr/bin/bash", "-euo", "pipefail", "-c"]):
        super().__init__()
        if isinstance(commands, ShellCommand):
            self.commands = [commands]
        else:
            for command in commands:
                assert isinstance(command, ShellCommand)
            self.commands = commands
        self.shell = shell

    def append(self, command):
        assert isinstance(command, ShellCommand)
        self.commands.append(command)

    def to_command(self):
        return self.shell + ["; ".join(str(command) for command in self.commands)]


class ShellCommand(object):
    def __init__(self, parts=[], stdin=None, stdout=None, stderr=None):
        self.parts = [shell_escape(str(part)) for part in parts]
        self.stdin(stdin)
        self.stdout(stdout)
        self.stderr(stderr)

    def append(self, part):
        self.parts.append(shell_escape(str(part)))

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
