from abc import ABC, abstractmethod
from inspect import signature
from pathlib import Path
from shlex import quote as shell_escape


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
        return self

    def __add__(self, part):
        return self.append(part)

    def stdin(self, file_path):
        self._stdin = self.__get_path(file_path)
        return self

    def stdout(self, file_path):
        self._stdout = self.__get_path(file_path)
        return self

    def stderr(self, file_path):
        self._stderr = self.__get_path(file_path)
        return self

    def __get_path(self, file_path):
        if file_path is None:
            return None
        else:
            return Path(file_path)

    def pipe(self, *new_parts):
        if len(new_parts) == 1 and isinstance(new_parts[0], ShellCommand):
            cmd = new_parts[0]

            # handle redirections
            if self._stderr is not None:
                self.parts.append(self.__redirect_op["stderr"])
                self.parts.append(shell_escape(str(self._stderr)))
            assert cmd._stdin is None
            self._stderr = cmd._stderr
            self._stdout = cmd._stdout

            # append pipe command
            self.parts.append("|")
            self.parts.extend(cmd.parts)
        else:
            self.parts.append("|")
            for new_part in new_parts:
                assert not isinstance(new_part, ShellCommand)
                self.append(new_part)
        return self

    def __or__(self, command):
        assert isinstance(command, ShellCommand)
        return self.pipe(command)

    def __str__(self):
        all_parts = [
            self.__redirect("stdin"),
            *self.parts,
            self.__redirect("stdout"),
            self.__redirect("stderr"),
        ]

        return " ".join(p for p in all_parts if p is not None)

    __redirect_op = {
        "stdin": "<",
        "stdout": ">",
        "stderr": "2>",
    }

    def __redirect(self, what):
        file_path = getattr(self, f"_{what}")

        if file_path is None:
            return None
        else:
            esc_file = shell_escape(str(file_path))
            op = self.__redirect_op[what]
            return f"{op} {esc_file}"


class PythonCode(AbstractAction):
    def __init__(self, function, name=None):
        super().__init__()
        if not callable(function):
            raise ValueError("function must be callable")
        if len(signature(function).parameters) > 0:
            raise ValueError("function must not take any arguments")
        self.function = function
        if name is None:
            self.name = self.function.__name__
        else:
            self.name = name

    def __call__(self):
        self.function()

    def to_command(self):
        raise NotImplementedError("PythonCode can only be executed locally")

    def __str__(self):
        return f"{self.name}()"


def python_code(function):
    def make_python_code_action():
        # propagate inputs, outputs, etc. to inner function
        function.__globals__.update(globals())
        return PythonCode(function)

    return make_python_code_action
