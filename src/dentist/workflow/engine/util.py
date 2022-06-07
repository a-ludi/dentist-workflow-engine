import inspect


def inject(function, required=[], **vars):
    params = inspect.signature(function).parameters

    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        # function takes any number of keyword arguments -> pass all vars
        injected_vars = vars
    else:
        injected_vars = dict()
        for key, value in vars.items():
            if key in params:
                injected_vars[key] = value
            elif key in required:
                raise ValueError(f"missing required argument `{key}`")

    def injected(**kwargs):
        new_kw = {**injected_vars, **kwargs}
        return function(**new_kw)

    injected.__name__ = function.__name__

    return injected


def throws(fun, *, exception_cls=Exception):
    try:
        fun()
    except exception_cls:
        return True

    return False
