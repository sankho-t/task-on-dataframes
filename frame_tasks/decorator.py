import inspect
import functools
import re
import warnings
from threading import RLock
from typing import Union, Optional, List

from .tasks import Task, tasks, current_interp_task, Variable, Var_In

# between new_task and close_task, global variable current_interp_task should not be modified
updating_task = RLock()


def requires(columns: List[Var_In], arg: str):
    assert isinstance(arg, int) or isinstance(arg, str)

    global current_interp_task
    if current_interp_task:
        for col in columns:
            current_interp_task.add_require(arg, col)

    def __f(f):
        x = inspect.stack()[1][3]
        doc = [f.__doc__ or ""]
        doc += [f"Requires columns {columns} for dataframe arg {arg}"]
        f.__doc__ = "\n".join(doc)

        @functools.wraps(f)
        def _f(*args, **kwargs):
            if isinstance(arg, int):
                x = args[arg]
            elif isinstance(arg, str):
                x = kwargs[arg]

            # c = set(columns).difference(x.columns) == set()
            # warnings.warn(f"Task for: {f.__name__}:: Missing from input: {c}")

            return f(*args, **kwargs)

        return _f

    return __f


def makes(columns: List[str], return_pos: Optional[int] = None, appends=True):
    assert columns

    global current_interp_task
    if current_interp_task:
        current_interp_task.appends = appends
        for col in columns:
            current_interp_task.add_generates(return_pos, col)

    def __f(f):
        doc = [f.__doc__ or ""]
        ret_at = f"at position {return_pos}" if return_pos else ""
        ret_app = ", along with the required input dataframe columns" if appends else ""
        doc += [f"Returns: dataframe {ret_at} with columns {columns} {ret_app}."]
        f.__doc__ = "\n".join(doc)

        @functools.wraps(f)
        def _f(*args, **kwargs):
            x = f(*args, **kwargs)
            x_ = x
            if return_pos:
                c = x_[return_pos]
            # c = set(columns).difference(x_.columns)
            # if c:
            # warnings.warn(f"Task for: {f.__name__}:: Missing from output: {c}")
            return x

        return _f

    return __f


def new_task(name: Optional[str] = None):
    updating_task.acquire()
    global current_interp_task
    current_interp_task = Task(name)
    fset = current_interp_task.set_function

    def _f(f):
        doc = [f.__doc__ or ""]
        doc = [f"Task: {name}"] + doc
        f.__doc__ = "\n".join(doc)

        @functools.wraps(f)
        def __f(*args, **kwargs):
            return f(*args, **kwargs)

        fset(f)

        return __f

    return _f


def close_task():
    global current_interp_task
    current_interp_task = None
    updating_task.release()

    def _f(f):
        @functools.wraps(f)
        def __f(*args, **kwargs):
            return f(*args, **kwargs)

        return __f

    return _f
