from itertools import chain
from pathlib import Path


class FileList:
    """Immutable list of file paths.

    The file list has some unique features:

    - items are converted with the following conventions:
        - item of type `FileList` are not converted
        - other items are tried to convert to `pathlib.Path`
        - if that fails, they are assumed to be iterables that are
          recursively converted to `FileList`s
    - items of the list may be named using named parameters.
    - `iter(file_list)` iterates over the individual paths implicitly
      flattening nested structures
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
    def _to_paths(item):
        if isinstance(item, FileList):
            return item
        try:
            return Path(item)
        except TypeError:
            if isinstance(item, dict):
                return FileList(**item)
            else:
                return FileList(*item)

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

    def keys(self):
        return chain(range(self._num_positional), self._index.keys())

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
            if isinstance(item, FileList):
                return str(item)
            else:
                return repr(str(item))

        names = list(self._index.keys())
        pos_items = (_item2str(item) for item in self._items[: self._num_positional])
        named_items = (
            f"{names[i]}={_item2str(item)}"
            for i, item in enumerate(self._items[self._num_positional :])
        )

        return f"FileList({', '.join(chain(pos_items, named_items))})"


class MultiIndex(tuple):
    DEFAULT_SEP = "."
    DEFAULT_RANGE_SEP = "-"

    def __new__(cls, *elements, sep=None, range_sep=None, collapse_ranges=True):
        for elem in elements:
            if not (
                isinstance(elem, int)
                or (
                    isinstance(elem, tuple)
                    and len(elem) == 2
                    and all(isinstance(e, int) for e in elem)
                )
            ):
                raise TypeError("MultiIndex elements must be int or tuple of two ints")

        obj = tuple.__new__(cls, elements)
        obj._sep = cls.DEFAULT_SEP if sep is None else str(sep)
        obj._range_sep = cls.DEFAULT_RANGE_SEP if range_sep is None else str(range_sep)
        obj._collapse_ranges = collapse_ranges

        return obj

    def values(self):
        if self._collapse_ranges and len(self) == 1 and not isinstance(self[0], int):
            return range(self[0][0], self[0][1] + 1)
        else:
            return MultiIndex._values_rec(self)

    @staticmethod
    def _values_rec(index):
        if len(index) == 0:
            return (tuple(),)

        if isinstance(index[0], int):
            head = (index[0],)
        else:
            head = range(index[0][0], index[0][1] + 1)

        return (
            MultiIndex(elem, *values)
            for elem in head
            for values in MultiIndex._values_rec(index[1:])
        )

    def to_str(self, sep=None, range_sep=None, collapse_ranges=None):
        sep = self._sep if sep is None else str(sep)
        range_sep = self._range_sep if range_sep is None else str(range_sep)
        collapse_ranges = (
            self._collapse_ranges if collapse_ranges is None else bool(collapse_ranges)
        )

        def elem2str(elem):
            if isinstance(elem, int):
                return str(elem)
            elif collapse_ranges and elem[0] == elem[1]:
                return str(elem[0])
            else:
                return f"{elem[0]}{range_sep}{elem[1]}"

        return sep.join(elem2str(elem) for elem in self)

    def __str__(self):
        return self.to_str()
