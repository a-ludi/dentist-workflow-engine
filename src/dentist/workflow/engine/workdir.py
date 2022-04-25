from pathlib import Path
from shutil import rmtree
import logging


__all__ = ["Workdir"]

log = logging.getLogger(__name__)


class Workdir(object):
    def __init__(self, root, parent=None):
        self.root = Path(root)

        if parent is None:
            self._registry = set()
        else:
            self._registry = parent._registry

    @staticmethod
    def _registered(create_path):
        def wrapper(self, path, *args, exist_ok=False, **kwargs):
            if path in self._registry:
                if not exist_ok:
                    raise Exception(f"workpath `{path}` has already been acquired")
            result = create_path(self, path, *args, exist_ok=exist_ok, **kwargs)
            self._registry.add(path)

            return result

        return wrapper

    @_registered
    def acquire_dir(self, path, force_empty=False, exist_ok=False):
        full_path = self.root / path

        if (force_empty or not exist_ok) and full_path.exists():
            try:
                rmtree(full_path)
            except Exception as reason:
                raise Exception(
                    f"Could not delete working directory: {reason}\n"
                    "\n"
                    f"Please delete it manually: {full_path}"
                )
        full_path.mkdir(parents=True, exist_ok=not force_empty and exist_ok)

        return Workdir(full_path, parent=self)

    @_registered
    def acquire_file(self, path, exist_ok=False):
        full_path = self.root / path

        if not exist_ok and full_path.exists():
            raise Exception(
                f"Working file already exists: {reason}\n"
                "\n"
                f"Please delete it manually: {full_path}"
            )

        self.acquire_dir(full_path.parent, exist_ok=True)

        return full_path
