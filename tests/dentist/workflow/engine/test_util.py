import pytest
from dentist.workflow.engine.util import inject


def test_inject():
    vars = {
        "a": 1,
        "b": 2,
        "c": 3,
    }
    test_required = {"a"}

    def fun1(a):
        return a == vars["a"]

    def fun2(a, b):
        return a == vars["a"] and b == vars["b"]

    def fun3(b, c):
        return b == vars["b"] and c == vars["c"]

    # test average use case
    assert inject(fun1, **vars)() is True
    assert inject(fun2, **vars)() is True
    assert inject(fun3, **vars)() is True

    # test required parameter
    assert inject(fun1, required=test_required, **vars)() is True
    assert inject(fun2, required=test_required, **vars)() is True
    with pytest.raises(ValueError):
        inject(fun3, required=test_required, **vars)

    # test additional args in secondary call
    assert inject(fun1)(a=vars["a"]) is True
    assert inject(fun2, a=vars["a"])(b=vars["b"]) is True
    assert inject(fun3, c=vars["c"])(b=vars["b"]) is True
