from itertools import chain
from pathlib import Path

from pytest import raises

from dentist.workflow.engine.container import FileList, MultiIndex


def _get_file_lists():
    l1 = FileList("0", "1", "2", "3")
    l2 = FileList(a="a", b="b", c="c")
    l3 = FileList("0", "1", "2", "3", a="a", b="b", c="c")
    l4 = FileList("0", ["1", "2", "3"], abc=list("abc"))
    l5 = FileList(abc=FileList(*"abc"))

    return l1, l2, l3, l4, l5


def test_file_list_iter():
    l1, l2, l3, l4, l5 = _get_file_lists()

    assert list(l1) == list(Path(i) for i in "0123")
    assert list(l2) == list(Path(i) for i in "abc")
    assert list(l3) == list(Path(i) for i in "0123abc")
    assert list(l4) == list(Path(i) for i in "0123abc")
    assert list(l5) == list(Path(i) for i in "abc")


def test_file_list_contains():
    l1, l2, l3, l4, l5 = _get_file_lists()

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

    for m in "abc":
        assert m in l5
    assert "5" not in l5
    assert "d" not in l5


def test_file_list_getitem():
    l1, l2, l3, l4, l5 = _get_file_lists()

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

    assert Path("a") == l5["abc"][0]
    assert Path("b") == l5["abc"][1]
    assert Path("c") == l5["abc"][2]
    with raises(IndexError):
        l5[0]
    with raises(KeyError):
        l5["a"]
    with raises(IndexError):
        l5["abc"][3]
    with raises(KeyError):
        l5["abc"]["a"]


def test_file_list_str():
    l1, l2, l3, l4, l5 = _get_file_lists()

    assert str(l1) == "FileList('0', '1', '2', '3')"
    assert str(l2) == "FileList(a='a', b='b', c='c')"
    assert str(l3) == "FileList('0', '1', '2', '3', a='a', b='b', c='c')"
    assert (
        str(l4) == "FileList('0', FileList('1', '2', '3'), abc=FileList('a', 'b', 'c'))"
    )
    assert str(l5) == "FileList(abc=FileList('a', 'b', 'c'))"


def test_file_list_from_any():
    l1 = FileList("abc")
    assert FileList.from_any(l1) == l1

    l2 = "foo"
    assert FileList.from_any(l2) == FileList(l2)

    l3 = dict(foo="foo")
    assert FileList.from_any(l3) == FileList(**l3)

    l4 = ["foo", "bar"]
    assert FileList.from_any(l4) == FileList(*l4)


def test_multi_index_new():
    mi = MultiIndex(1, 2, 3)
    assert mi[0] == 1
    assert mi[1] == 2
    assert mi[2] == 3
    assert mi._sep == "."


def test_multi_index_str():
    mi1 = MultiIndex(1, 2, 3)
    assert str(mi1) == "1.2.3"

    mi2 = MultiIndex("a", "b", "c", sep="|")
    assert str(mi2) == "a|b|c"


def test_multi_index_eq_with_tuple():
    mi1 = MultiIndex(1, 2, 3)
    assert mi1 == (1, 2, 3)

    mi2 = MultiIndex("a", "b", "c", sep="|")
    assert mi2 == ("a", "b", "c")


def test_multi_index_hash_with_tuple():
    mi1 = MultiIndex(1, 2, 3)
    mi2 = MultiIndex("a", "b", "c", sep="|")
    d = {mi1: 1, mi2: 2}

    assert mi1 in d
    assert (1, 2, 3) in d
    assert mi2 in d
    assert ("a", "b", "c") in d
