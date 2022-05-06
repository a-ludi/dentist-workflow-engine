from pathlib import Path
from shlex import quote as shell_escape


class RootResources(object):
    mime_types = {
        ".yml": "text/yaml",
        ".yaml": "text/yaml",
        ".json": "application/json",
    }

    @staticmethod
    def _enforce_dict(data, where):
        if not isinstance(data, dict):
            raise Exception("resources file must contain an object/dict at {where}")

    def __init__(self, data={}):
        self._enforce_dict(data, "the root")
        self._default = data.get("__default__", dict())
        self._default.setdefault("ncpus", 1)
        self._enforce_dict(self._default, "__default__")
        self._data = data

    @classmethod
    def read(cls, path):
        path = Path(path)
        mime_type = cls.mime_types.get(path.suffix, None)

        if mime_type is None:
            extensions = ", ".join(f"`{ext}`" for ext in cls.mime_types.keys())
            raise Exception(
                f"resources file extension must be one of {extensions} "
                f"but got `{path.suffix}`"
            )

        if mime_type == "text/yaml":
            from yaml import safe_load as load
        elif mime_type == "application/json":
            from json import load
        else:
            assert False

        with open(path) as res:
            return RootResources(load(res))

    def __getitem__(self, job_name):
        res = self._default.copy()
        res.update(self._data.get(job_name, {}))

        return Resources(res)


class Resources(dict):
    def to_cli(
        self,
        short_opt_prefix="-",
        short_opt_sep="",
        long_opt_prefix="--",
        long_opt_sep="=",
        tr={},
    ):
        def to_str(key, value):
            key = str(key)
            key = tr.get(key, key)
            is_short = len(key) == 1
            prefix = str(short_opt_prefix if is_short else long_opt_prefix)
            sep = str(short_opt_sep if is_short else long_opt_sep)
            value = str(value)

            return prefix + key + sep + value

        return [shell_escape(to_str(key, value)) for key, value in self.items()]
