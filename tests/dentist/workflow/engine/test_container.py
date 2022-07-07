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
    mi1 = MultiIndex(1, 2, 3)
    assert mi1[0] == 1
    assert mi1[1] == 2
    assert mi1[2] == 3
    assert mi1._sep == MultiIndex.DEFAULT_SEP
    assert mi1._range_sep == MultiIndex.DEFAULT_RANGE_SEP

    mi2 = MultiIndex(1, (2, 4), 3, sep="|", range_sep="_")
    assert mi2[0] == 1
    assert mi2[1] == (2, 4)
    assert mi2[2] == 3
    assert mi2._sep == "|"
    assert mi2._range_sep == "_"

    with raises(TypeError):
        MultiIndex(1, (2, 4, 5), 3)

    with raises(TypeError):
        MultiIndex(1, "a", 3)


def test_multi_index_values():
    mi1 = MultiIndex(1, 2, 3)
    assert list(mi1.values()) == [(1, 2, 3)]

    mi2 = MultiIndex((1, 2))
    assert list(mi2.values()) == [1, 2]

    mi3 = MultiIndex((1, 2), collapse_ranges=False)
    assert list(mi3.values()) == [(1,), (2,)]

    mi4 = MultiIndex(1, (2, 4), 3)
    assert list(mi4.values()) == [
        (1, 2, 3),
        (1, 3, 3),
        (1, 4, 3),
    ]

    mi5 = MultiIndex(1, (2, 4), (3, 3))
    assert list(mi5.values()) == [
        (1, 2, 3),
        (1, 3, 3),
        (1, 4, 3),
    ]

    mi6 = MultiIndex(1, (2, 4), (3, 5))
    assert list(mi6.values()) == [
        (1, 2, 3),
        (1, 2, 4),
        (1, 2, 5),
        (1, 3, 3),
        (1, 3, 4),
        (1, 3, 5),
        (1, 4, 3),
        (1, 4, 4),
        (1, 4, 5),
    ]


def test_multi_index_str():
    mi1 = MultiIndex(1, 2, 3)
    assert str(mi1) == "1.2.3"

    mi2 = MultiIndex(1, 2, 3, sep="|")
    assert str(mi2) == "1|2|3"

    mi3 = MultiIndex(1, (2, 4), 3)
    assert str(mi3) == "1.2-4.3"

    mi4 = MultiIndex(1, (2, 4), (3, 3))
    assert str(mi4) == "1.2-4.3"

    mi5 = MultiIndex(1, (2, 4), (3, 3), collapse_ranges=False)
    assert str(mi5) == "1.2-4.3-3"


def test_multi_index_eq_with_tuple():
    mi1 = MultiIndex(1, 2, 3)
    assert mi1 == (1, 2, 3)

    mi2 = MultiIndex(1, 2, 3, sep="|")
    assert mi2 == (1, 2, 3)

    mi3 = MultiIndex(1, (2, 4), 3)
    assert mi3 == (1, (2, 4), 3)


def test_multi_index_hash_with_tuple():
    mi1 = MultiIndex(1, 2, 3)
    mi2 = MultiIndex(1, 2, 3, sep="|")
    mi3 = MultiIndex(1, (2, 4), 3)
    d = {mi1: 1, mi2: 2, mi3: 3}

    assert mi1 in d
    assert (1, 2, 3) in d
    assert mi2 in d
    assert (1, 2, 3) in d
    assert mi3 in d
    assert (1, (2, 4), 3) in d


def test_multi_index_inheritance():
    class MyMultiIndex(MultiIndex):
        DEFAULT_SEP = "|"
        DEFAULT_RANGE_SEP = "_"

    ri = MyMultiIndex(1, (2, 3))
    assert str(ri) == "1|2_3"
