from abc import ABC, abstractmethod
from pathlib import Path
from shlex import quote as shell_escape

from .util import inject


class AbstractAction(ABC):
    local_only = False

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


class safe(str):
    """Mark a string as safe, i.e. it will not be escaped."""

    pass


class ShellScript(AbstractAction):
    def __init__(
        self, *lines, shell=["/bin/bash", "-c"], safe_mode="set -euo pipefail"
    ):
        super().__init__()
        self.lines = lines
        self.shell = shell
        self.safe_mode = safe_mode

    def append(self, *lines):
        self.lines.extend(lines)

    def to_command(self):
        script = ShellScript._make_script(self.lines)
        if self.safe_mode is not None:
            script = f"{self.safe_mode}; {script}"

        if self.tracking_status_path is not None:
            status_path = shell_escape(str(self.tracking_status_path))
            preface = f"touch {status_path}"
            epilogue = f"S=$?; echo $S > {status_path}; exit $S"
            script = f"{preface}; ( {script} ); {epilogue}"

        return [*self.shell, script]

    @staticmethod
    def _make_script(lines):
        return "\n".join(ShellScript._make_line(line) for line in lines)

    @staticmethod
    def _make_line(line):
        if isinstance(line, tuple):
            return " ".join(ShellScript._escape(fragment) for fragment in line)
        else:
            return ShellScript._escape(line)

    @staticmethod
    def _escape(fragment):
        if isinstance(fragment, safe):
            return fragment
        else:
            return shell_escape(str(fragment))


class PythonCode(AbstractAction):
    local_only = True

    def __init__(self, function, name=None):
        super().__init__()
        if not callable(function):
            raise ValueError("function must be callable")
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
    def make_python_code_action(**vars):
        # propagate inputs, outputs, etc. to inner function
        return PythonCode(inject(function, **vars))

    make_python_code_action.__name__ = function.__name__

    return make_python_code_action
