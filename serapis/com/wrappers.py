

import functools
import inspect


def check_args_not_none(funct):
    @functools.wraps(funct)
    def wrapper(*args, **kwargs):
        func_args = inspect.getcallargs(funct, *args, **kwargs)
        none_args = [(arg, val) for arg, val in func_args.items() if val is None]
        if none_args:
            msg = "None arguments have been provided for this function: "+str(func_args)
            raise ValueError(msg)
        return funct(*args, **kwargs)
    return wrapper
