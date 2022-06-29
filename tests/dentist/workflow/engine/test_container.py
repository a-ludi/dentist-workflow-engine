from itertools import chain
from pathlib import Path

from pytest import raises

from dentist.workflow.engine.container import FileList


def _get_file_lists():
    l1 = FileList("0", "1", "2", "3")
    l2 = FileList(a="a", b="b", c="c")
    l3 = FileList("0", "1", "2", "3", a="a", b="b", c="c")
    l4 = FileList("0", ["1", "2", "3"], abc=list("abc"))

    return l1, l2, l3, l4


def test_file_list_iter():
    l1, l2, l3, l4 = _get_file_lists()

    assert list(l1) == list(Path(i) for i in "0123")
    assert list(l2) == list(Path(i) for i in "abc")
    assert list(l3) == list(Path(i) for i in "0123abc")
    assert list(l4) == list(Path(i) for i in "0123abc")


def test_file_list_contains():
    l1, l2, l3, l4 = _get_file_lists()

    for i in "0123":
        assert i in l1
    assert "5" not in l1

    for c in "abc":
        assert c in l2
    assert "d" not in l2

    for m in "0123abc":
        assert m in l3
    assert "5" not in l3
    assert "d" not in l3

    for m in "0123abc":
        assert m in l4
    assert "5" not in l4
    assert "d" not in l4


def test_file_list_getitem():
    l1, l2, l3, l4 = _get_file_lists()

    for i in range(4):
        assert Path(str(i)) == l1[i]
    with raises(IndexError):
        l1[4]
    with raises(KeyError):
        l1["a"]

    for c in "abc":
        assert Path(c) == l2[c]
    with raises(IndexError):
        l2[0]
    with raises(KeyError):
        l2["d"]

    for m in chain(range(4), "abc"):
        assert Path(str(m)) == l3[m]
    with raises(IndexError):
        l3[4]
    with raises(KeyError):
        l3["d"]

    assert Path("0") == l4[0]
    assert tuple(Path(i) for i in "123") == l4[1]
    assert tuple(Path(c) for c in "abc") == l4["abc"]
    with raises(IndexError):
        l4[2]
    with raises(KeyError):
        l4["a"]


def test_file_list_str():
    l1, l2, l3, l4 = _get_file_lists()

    assert str(l1) == "FileList('0', '1', '2', '3')"
    assert str(l2) == "FileList(a='a', b='b', c='c')"
    assert str(l3) == "FileList('0', '1', '2', '3', a='a', b='b', c='c')"
    assert str(l4) == "FileList('0', ['1', '2', '3'], abc=['a', 'b', 'c'])"


def test_file_list_from_any():
    l1 = FileList("abc")
    assert FileList.from_any(l1) == l1

    l2 = "foo"
    assert FileList.from_any(l2) == FileList(l2)

    l3 = dict(foo="foo")
    assert FileList.from_any(l3) == FileList(**l3)

    l4 = ["foo", "bar"]
    assert FileList.from_any(l4) == FileList(*l4)
