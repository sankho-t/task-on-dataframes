"Define tasks"

import re
import sys
import warnings
from copy import copy, deepcopy
from itertools import groupby
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
            try:
                return self.string == x
            except AttributeError:
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

    def reindex(self, columns=List[str]):
        ...


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
        reindex: Dict[Arg, List[str]] = dict(
            map(
                lambda x: (x[0], [None] * len(list(x[1]))),
                groupby(self.requires, key=lambda x: x[0]),
            )
        )

        data_pass = {}
        for (data_i, data_col), (arg, arg_col) in req_map.items():
            data_pass[arg] = copy(data[data_i])
            ident: Union[re.Pattern, str] = arg_col.matcher
            try:
                ident = arg_col.string
            except AttributeError:
                pass
            reference[(arg, ident)] = data_col
            arg_reqs = filter(lambda x: x[0] == arg, self.requires)
            refer_pos = next(
                filter(
                    lambda x: (
                        (x[1][1] == arg_col) or (x[1][1].matcher == arg_col.matcher)
                    ),
                    enumerate(arg_reqs),
                )
            )
            pos: int = refer_pos[0]
            reindex[arg][pos] = data_col

        for arg in reindex.keys():
            absent = set(reindex[arg]).difference(data_pass[arg].columns)
            assert all(reindex[arg])
            if absent:
                warnings.warn(f"Executing {self.fname}: {absent} not found for {arg}")
            kwargs[arg] = data_pass[arg].reindex(columns=reindex[arg])

        if self.pass_extra is not False:
            if "requires" in kwargs:
                del kwargs["requires"]
            if "expects" in kwargs:
                del kwargs["expects"]
            assert "requires" not in kwargs
            assert "expects" not in kwargs
            output_ = self.fcode(requires=reference, expects=expects, **kwargs)
        else:
            output_ = self.fcode(**kwargs)  # type: ignore

        if expects:
            if any([x[0] for x in expects]):
                if not isinstance(output_, BaseData) and isinstance(output_, Iterable):
                    for i, exp in groupby(expects, key=lambda x: x[0]):
                        try:
                            op = output_[i]
                        except IndexError:
                            warnings.warn(
                                f"Return from {self.fname}: returns less than expected"
                            )
                            break
                        exp_ = set(map(lambda x: x[1], exp))
                        absent = exp_.difference(op.columns)
                        if absent:
                            warnings.warn(
                                f"Return from {self.fname}: {absent} not found in position {i}"
                            )
                    output = list(output_)
                else:
                    warnings.warn(f"Return from {self.fname}: expected iterable")
                    output = [output_]
            else:
                op = output_
                if self.appends and len(reindex) == 1:
                    extras = (
                        data_pass[arg]
                        .drop_duplicates(subset=reindex[arg])
                        .set_index(reindex[arg])
                    )
                    if not extras.empty:
                        op = op.join(extras, on=reindex[arg])
                exp_ = set(map(lambda x: x[1], expects))
                absent = exp_.difference(op.columns)
                if absent:
                    warnings.warn(f"Return from {self.fname}: {absent} not found")
                output = [output_]
        return output


global current_interp_task
current_interp_task: Optional[Task] = None


class NotSolvable(RuntimeError):
    pass


class BadTask(RuntimeError):
    pass


HaveVars = Dict[int, List[str]]


class TaskCaller:
    def __init__(self, have: HaveVars, for_task: Task):
        self.have = have
        # map have[i][j] to Task: argument, variable
        self.mapped: CallReqsMap = {}
        self.satisfied = False
        self.task_generates: List[RetArg] = list(for_task.generates)
        self.gen_appends = for_task.appends
        self.task_name = for_task.fname

        has_dep_reqs = set()
        for xa, xr in for_task.requires:
            try:
                if re.search(r"{.*?}", xr.string):
                    has_dep_reqs.add(xa)
            except AttributeError:
                pass

        if for_task.requires and has_dep_reqs == set(
            map(lambda x: x[0], for_task.requires)
        ):
            raise BadTask(f"All requirments for task {for_task} are dynamic")
        self.task_requires: List[Tuple[str, Variable]] = sorted(
            for_task.requires, key=lambda x: x[0] not in has_dep_reqs
        )

        self.len_requires = len(self.task_requires)

    def satisfy(self) -> Iterator[Tuple[CallReqsMap, List[RetArg]]]:
        for x in self.satisfy_requires():
            if len(x) < self.len_requires:
                continue
            try:
                y = self.get_generates(x)
            except NotSolvable:
                continue
            yield (x, y)

    def satisfy_requires(self) -> Iterator[CallReqsMap]:

        if self.task_requires:
            arg, var = self.task_requires.pop()
            have_items = list(self.have.items())
            if arg in map(lambda x: x[0], self.mapped.values()):
                mapped_arg = filter(lambda x: x[1][0] == arg, self.mapped.items())
                mapped_ind: int = next(mapped_arg)[0][0]
                have_items = list(filter(lambda x: x[0] == mapped_ind, have_items))
            for have_arg, ha_vars in have_items:
                for x in ha_vars:
                    try:
                        re.search(r"{.*?}", var.string)
                    except AttributeError:
                        pass
                    else:
                        try:
                            var.string = self.replace_name_with_req(
                                var.string, self.mapped
                            )
                        except NotSolvable:
                            continue
                    if var == x:
                        task_req0 = self.task_requires.copy()
                        z = None
                        if (have_arg, x) in self.mapped:
                            z = self.mapped[(have_arg, x)]
                        self.mapped[(have_arg, x)] = arg, var
                        for option in self.satisfy_requires():
                            ret = dict(
                                [((have_arg, x), (arg, var))] + list(option.items())
                            )
                            yield ret
                        if z:
                            self.mapped[(have_arg, x)] = z
                        else:
                            del self.mapped[(have_arg, x)]
                        self.task_requires = task_req0
        else:
            yield {}

    @staticmethod
    def replace_name_with_req(name: str, req: CallReqsMap) -> str:
        def replace_with_req(m) -> str:
            grp = iter(m.groups())
            arg = next(grp)
            try:
                var_ind = int(next(grp))
            except (StopIteration, TypeError):
                var_ind = 0
            try:
                match_ind = int(next(grp))
            except (StopIteration, TypeError):
                match_ind = 0

            try:
                refer = [x for x in req.items() if x[1][0] == arg][var_ind]
            except IndexError:
                raise NotSolvable()
            refer_var: Variable = refer[1][1]
            _caller_arg, caller_var = refer[0]

            m2 = re.match(refer_var.matcher, caller_var)

            if not m2:
                raise NotSolvable(
                    f"{name} does not match with {refer_var} and {caller_var}"
                )
            return m2.groups()[match_ind]

        return re.sub(r"{(\w+)(?:\.(\d+)(?:\.(\d+))?)?}", replace_with_req, name)

    def get_generates(self, requires_satisfied: CallReqsMap) -> List[RetArg]:

        generates = []
        for (index, c) in self.task_generates:

            gen_new = self.replace_name_with_req(c, requires_satisfied)
            generates.append((index, gen_new))

        if self.gen_appends:
            assert len(set(map(lambda x: x[0], requires_satisfied.keys()))) <= 1
            cols: List[str] = next(iter(self.have.values()))
            for ind in set(map(lambda x: x[0], generates)):
                for col in cols:
                    if (ind, col) not in generates:
                        generates.append((ind, col))
        return generates


def test_call(
    x: List[BaseData], for_task: Task
) -> Iterator[Tuple[CallReqsMap, List[RetArg]]]:
    hv = {}
    for i, xx in enumerate(x):
        hv[i] = list(xx.columns)
    task_caller = TaskCaller(hv, for_task)

    return task_caller.satisfy()
