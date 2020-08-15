import click

from collections import defaultdict
from itertools import groupby
from typing import Iterable, List, Tuple, FrozenSet, Optional, NamedTuple, DefaultDict

from simpleai.search import SearchProblem, breadth_first
from .tasks import CallReqsMap, tasks, RetArg, TaskCaller, BaseData


MAX_REPEAT_GENERIC_TASK: Optional[int] = 1

State = NamedTuple(
    "State", [("Vars", Tuple[FrozenSet[str], ...]), ("Tasks", Tuple[str, ...])]
)

Action = NamedTuple(
    "Action", [("Task", str), ("CallMap", CallReqsMap), ("Returns", List[RetArg])]
)

Action.returns_int = lambda self: [
    (-1, x[1]) if x[0] is None else x for x in self.Returns
]

Action.callmap_flat = lambda self: [(*x[0], *x[1]) for x in self.CallMap.items()]


def actions_given_state(state: State) -> Iterable[Action]:
    found_actions = []

    state_vars = state.Vars
    havevars = dict(map(lambda x: (x[0], list(x[1])), enumerate(state_vars)))

    ignore_tasks = []
    if MAX_REPEAT_GENERIC_TASK is not None:
        gen_tasks: DefaultDict[str, int] = defaultdict(int)
        for taskn in state.Tasks:
            if tasks[taskn].is_generic():
                gen_tasks[taskn] += 1
                if gen_tasks[taskn] >= MAX_REPEAT_GENERIC_TASK:
                    ignore_tasks.append(taskn)

    for key, task in tasks.items():
        if key not in ignore_tasks:
            tc = TaskCaller(havevars, for_task=task)
            for callmap, returns in tc.satisfy():
                for _, vars in groupby(returns, key=lambda x: x[0]):
                    this_rets = frozenset(map(lambda x: x[1], vars))
                    if not this_rets in state_vars:
                        break
                else:
                    continue
                found_actions.append(Action(Task=key, CallMap=callmap, Returns=returns))

    return found_actions


def apply_action(state: State, action: Action) -> State:
    returns = action.Returns
    state2 = []

    for _, g in groupby(returns, key=lambda x: x[0]):
        state2.append(frozenset(map(lambda x: x[1], g)))

    tasks = tuple([*state.Tasks, action.Task])
    state_n = tuple([*state.Vars, *state2])
    return State(Vars=state_n, Tasks=tasks)


def apply_many_actions(state: State, action: Iterable[Action]) -> State:
    for act in action:
        state = apply_action(state, act)
    return state


class TaskProblem(SearchProblem):
    def __init__(self, goal: List[List[str]], initial_vars: List[List[str]]):
        self.goal = goal
        init_st = tuple(map(lambda x: frozenset(x), initial_vars))

        super().__init__(initial_state=State(Vars=init_st, Tasks=tuple()))

    def actions(self, state: State) -> Iterable[Action]:
        return actions_given_state(state)

    def result(self, state: State, action: Action) -> State:
        return apply_action(state, action)

    def is_goal(self, state: State) -> bool:
        for x in self.goal:
            for y in state.Vars:
                if all(xx in y for xx in x):
                    break
            else:
                break
        else:
            return True
        return False

    def heuristic(self, state: State) -> int:
        return 1


TaskExec = Iterable[Tuple[Optional[Action], State]]


def find_path(source: List[List[str]], dest: List[List[str]]) -> TaskExec:

    tp = TaskProblem(goal=dest, initial_vars=source)

    result = breadth_first(tp, graph_search=True)
    if result:
        return result.path()
    return []


def perform_actions(
    sources: List[BaseData], actions: Iterable[Action], return_latest_first=True
) -> Iterable[BaseData]:

    current_data = sources

    with click.progressbar(actions) as actions_:
        for action in actions_:
            task = tasks[action.Task]
            retn = task.call_task(action.CallMap, action.Returns, current_data)
            current_data.extend(retn)

    if return_latest_first:
        return reversed(current_data)
    return current_data


def Executor(
    sources: List[BaseData], build: List[List[str]], show_progress=True
) -> List[BaseData]:
    source = [[xx for xx in x.columns if isinstance(xx, str)] for x in sources]

    path = find_path(source, build)
    if not path:
        raise RuntimeError("Path not found")

    res = perform_actions(
        sources, [act for act, _ in path if act], return_latest_first=False,
    )

    return list(res)
