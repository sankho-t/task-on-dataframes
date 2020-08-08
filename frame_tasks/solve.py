from collections import defaultdict
from itertools import groupby
from typing import Iterable, List, Tuple, FrozenSet, Optional, NamedTuple, DefaultDict

from simpleai.search import SearchProblem, breadth_first
from tqdm.auto import tqdm
from .tasks import CallReqsMap, tasks, RetArg, TaskCaller, BaseData


MAX_REPEAT_GENERIC_TASK: Optional[int] = 1

State = NamedTuple(
    "State", [("Vars", Tuple[FrozenSet[str], ...]), ("Tasks", Tuple[str, ...])]
)

Action = NamedTuple(
    "Action", [("Task", str), ("CallMap", CallReqsMap), ("Returns", List[RetArg])]
)


class TaskProblem(SearchProblem):
    def __init__(self, goal: List[List[str]], initial_vars: List[List[str]]):
        self.goal = goal
        init_st = tuple(map(lambda x: frozenset(x), initial_vars))

        super().__init__(initial_state=State(Vars=init_st, Tasks=tuple()))

    def actions(self, state: State) -> Iterable[Action]:
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
                    found_actions.append(
                        Action(Task=key, CallMap=callmap, Returns=returns)
                    )

        return found_actions

    def result(self, state: State, action: Action) -> State:
        returns = action.Returns
        state2 = []

        for _, g in groupby(returns, key=lambda x: x[0]):
            state2.append(frozenset(map(lambda x: x[1], g)))

        tasks = tuple([*state.Tasks, action.Task])
        state_n = tuple([*state.Vars, *state2])
        return State(Vars=state_n, Tasks=tasks)

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


def Executor(
    sources: List[BaseData], build: List[List[str]], show_progress=True
) -> List[BaseData]:
    source = [[xx for xx in x.columns if isinstance(xx, str)] for x in sources]

    current_data = sources

    path = find_path(source, build)
    if not path:
        raise RuntimeError("Path not found")
    if show_progress:
        path = tqdm(path)
    for action, _state in path:
        if action:
            task = tasks[action.Task]
            retn = task.call_task(action.CallMap, action.Returns, current_data)
            current_data.extend(retn)

    return current_data
