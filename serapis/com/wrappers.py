

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


def one_argument_only(funct):
    """
    This function decorator is used to annotate functions/methods that take a list of optional
    parameters and should have exactly one non empty parameter out of all the list.
    """
    @functools.wraps(funct)
    def wrapper(*args, **kwargs):
        func_args = inspect.getcallargs(funct, *args, **kwargs)
        non_empty_args = [(arg, val) for arg, val in func_args.items() if val is not None]
        if len(non_empty_args) != 1:
            msg = "This function should be called with exactly 1 parameter from the optional parameters list"
            raise ValueError(msg)
        return funct(*args, **kwargs)
    return wrapper
