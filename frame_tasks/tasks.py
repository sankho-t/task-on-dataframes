"Define tasks"

import re
import sys
from copy import copy, deepcopy
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Union


ver_maj, ver_min = list(map(int, sys.version.split(".")[:2]))
if ver_maj == 3 and ver_min < 8:
    from typing_extensions import Protocol  # type: ignore
else:
    from typing import Protocol  # type: ignore


tasks: Dict[str, "Task"] = {}

variable_match_ignore_case = False
Var_In = Union[re.Pattern, str]


class Variable:
    def __init__(self, x: Union[str, re.Pattern]):
        if isinstance(x, str):
            re_f = {"flags": re.I} if variable_match_ignore_case else {}
            self.matcher = re.compile(x, **re_f)
            self.string = x
        else:
            self.matcher = x

    def __hash__(self):
        return hash(self.matcher)

    def __eq__(self, x: object) -> bool:
        if isinstance(x, str):
            return re.match(self.matcher, x) is not None
        elif isinstance(x, Variable):
            try:
                return self == x.string
            except AttributeError:
                return x.matcher == self.matcher
        return False

    def __repr__(self) -> str:
        try:
            return self.string
        except AttributeError:
            return str(self.matcher)


class BaseData(Protocol):
    columns: Iterable[Union[str, int]]


Arg = str
# Caller: argument vs variable
CallArg = Tuple[Arg, Variable]
CallTimeArgs = List[Tuple[Arg, str]]
# When calling a task, which argument to be passed as which parameter
CallMap = Dict[Arg, Dict[str, Variable]]
# Task should return these variables in these return positions
RetArg = Tuple[Optional[int], str]

# Source argument, variable -> Task argument, variable
CallReqsMap = Dict[Tuple[int, str], Tuple[Arg, Variable]]


class TaskableFunc(Protocol):
    def __call__(
        self,
        /,
        requires: Dict[Tuple[str, Union[re.Pattern, str]], str] = {},
        expects: List[Tuple[Optional[int], str]] = [],
        **data: BaseData,
    ) -> Union[BaseData, List[BaseData]]:
        ...


class Task:
    def __init__(self, ref: Optional[str]):
        self.requires: List[CallArg] = []
        self.generates: List[RetArg] = []
        self.fcode: Optional[TaskableFunc] = None
        self.ref = ref
        self.appends = False
        self.pass_extra: Optional[bool] = None
        self.is_generic_ = False

    def is_generic(self) -> bool:
        if not self.is_generic_:
            return bool(self.requires)
        return True

    def add_require(self, arg: Arg, col: Var_In):
        if isinstance(col, re.Pattern):
            self.is_generic_ = True
            if self.pass_extra is None:
                self.pass_extra = True

        self.requires.append((arg, Variable(col)))

    def add_generates(self, posn: Optional[int], col: str):
        self.generates.append((posn, col))

    def __repr__(self):
        return f"{self.fname}:{self.requires} -> {self.generates}"

    def set_function(self, f):
        self.fname = f.__name__
        self.fcode = f
        tasks[self.fname] = self

    def call_task(
        self, req_map: CallReqsMap, expects: List[RetArg], data: List[BaseData]
    ) -> List[BaseData]:

        if self.fcode is None:
            raise RuntimeError("Function is not set in task!")
        kwargs = {}
        reference = {}
        for (data_i, data_col), (arg, arg_col) in req_map.items():
            kwargs[arg] = copy(data[data_i])
            ident: Union[re.Pattern, str] = arg_col.matcher
            try:
                ident = arg_col.string
            except AttributeError:
                pass
            reference[(arg, ident)] = data_col

        if self.pass_extra:
            if "requires" in kwargs:
                del kwargs["requires"]
            if "expects" in kwargs:
                del kwargs["expects"]
            assert "requires" not in kwargs
            assert "expects" not in kwargs
            output = self.fcode(requires=reference, expects=expects, **kwargs)
        else:
            output = self.fcode(**kwargs)  # type: ignore
        if not isinstance(output, list):
            return [output]
        return output


global current_interp_task
current_interp_task: Optional[Task] = None


class NotSolvable(RuntimeError):
    pass


HaveVars = Dict[int, List[str]]


class TaskCaller:
    def __init__(self, have: HaveVars, for_task: Task):
        self.have = have
        # map have[i][j] to Task: argument, variable
        self.mapped: CallReqsMap = {}
        self.satisfied = False
        self.task_requires: List[Tuple[str, Variable]] = list(for_task.requires)
        self.task_generates: List[RetArg] = list(for_task.generates)
        self.gen_appends = for_task.appends

    def satisfy(self) -> Iterator[Tuple[CallReqsMap, List[RetArg]]]:
        for x in self.satisfy_requires():
            try:
                y = self.get_generates(x)
            except NotSolvable:
                pass
            yield (x, y)

    def satisfy_requires(self) -> Iterator[CallReqsMap]:

        if self.task_requires:
            arg, var = self.task_requires.pop()
            have_items = list(self.have.items())
            if arg in map(lambda x: x[0], self.mapped.values()):
                have_items = list(filter(lambda x: x[0] == arg, have_items))
            for have_arg, ha_vars in have_items:
                for x in ha_vars:
                    if var == x:
                        caller2 = deepcopy(self)
                        caller2.mapped[(have_arg, x)] = arg, var
                        for option in caller2.satisfy_requires():
                            yield dict(
                                [((have_arg, x), (arg, var))] + list(option.items())
                            )
        else:
            yield {}

    def get_generates(self, requires_satisfied: CallReqsMap) -> List[RetArg]:
        def replace_with_req(m) -> str:
            arg, var_ind_, match_ind_ = m.groups()
            # indices that refer to the require variable
            var_ind, match_ind = int(var_ind_), int(match_ind_)

            refer = [x for x in requires_satisfied.items() if x[1][0] == arg][var_ind]
            refer_var: Variable = refer[1][1]
            _caller_arg, caller_var = refer[0]

            m2 = re.match(refer_var.matcher, caller_var)

            if not m2:
                raise NotSolvable(
                    f"{c} does not match with {refer_var} and {caller_var}"
                )
            return m2.groups()[match_ind]

        generates = []
        for (index, c) in self.task_generates:

            gen_new = re.sub(r"{(\w+)\.(\d+)\.(\d+)}", replace_with_req, c)
            generates.append((index, gen_new))

        if self.gen_appends:
            assert len(set(map(lambda x: x[0], requires_satisfied.keys()))) <= 1
            cols: List[str] = next(iter(self.have.values()))
            for ind in set(map(lambda x: x[0], generates)):
                for col in cols:
                    generates.append((ind, col))
        return generates
