from itertools import chain
from pathlib import Path


class FileList:
    """Immutable list of file paths.

    The file list has some unique features:

    - items are converted to `pathlib.Path`, or to tuples of `pathlib.Path`
      if an iterable is given
    - items of the list may be named using named parameters.
    - `iter(file_list)` iterates over the individual paths implicitly
      flattening lists/tuples
    """

    def __init__(self, *items, **named_items):
        self._num_positional = len(items)
        items = chain(items, named_items.values())
        self._items = tuple(FileList._to_paths(item) for item in items)
        self._index = dict(
            (key, idx)
            for idx, key in enumerate(named_items.keys(), self._num_positional)
        )
        self._len = sum(
            len(item) if isinstance(item, list) else 1 for item in self._items
        )

    @staticmethod
    def _to_paths(item, depth=0):
        if depth > 1:
            raise ValueError("invalid file list item: nested lists are not supported")

        try:
            return Path(item)
        except TypeError:
            return tuple(FileList._to_paths(sub_item, depth + 1) for sub_item in item)

    @staticmethod
    def from_any(container):
        if isinstance(container, FileList):
            # pass through if already a file list
            return container

        try:
            # try creating a single-valued file list
            return FileList(Path(container))
        except TypeError:
            pass

        if isinstance(container, dict):
            # treat dicts as named items
            return FileList(**container)
        else:
            # treat everything else as a sequence of items
            return FileList(*container)

    def __iter__(self):
        for item in self._items:
            if isinstance(item, Path):
                yield item
            else:
                for sub_item in item:
                    yield sub_item

    def __len__(self):
        return self._len

    def __contains__(self, value):
        value = Path(value)
        return any(item == value for item in iter(self))

    def __eq__(self, other):
        try:
            other = FileList.from_any(other)
            return self._items == other._items and self._index == other._index
        except Exception:
            raise TypeError(
                f"cannot compare object of type {type(other)} with FileList"
            )

    def __getitem__(self, key):
        return self._items[self._lookup(key)]

    def __getattr__(self, attr):
        return self[attr]

    def _lookup(self, key):
        if isinstance(key, int):
            if 0 <= key and key < self._num_positional:
                return key
            else:
                raise IndexError("named list index out of range")
        elif isinstance(key, str):
            index = self._index.get(key, None)
            if index is not None:
                return index
            else:
                raise KeyError(key)
        else:
            raise ValueError("index must be int or str")

    def __str__(self):
        def _item2str(item):
            if isinstance(item, tuple):
                return f"[{', '.join(_item2str(sub_item) for sub_item in item)}]"
            else:
                return repr(str(item))

        names = list(self._index.keys())
        pos_items = (_item2str(item) for item in self._items[: self._num_positional])
        named_items = (
            f"{names[i]}={_item2str(item)}"
            for i, item in enumerate(self._items[self._num_positional :])
        )

        return f"FileList({', '.join(chain(pos_items, named_items))})"
