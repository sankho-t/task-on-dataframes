import re
import pathlib
import urllib.parse
from typing import Iterable, Optional, List, Dict

import pandas as pd

from .tasks import Variable
from .solve import (
    State,
    Action,
    perform_actions,
    actions_given_state,
    RetArg,
    apply_many_actions,
)


def load_data(path: str, head1: bool = False) -> Optional[pd.DataFrame]:
    path_ = pathlib.Path(path)
    if path_.exists():
        if path_.suffix.lower() == ".csv":
            return pd.read_csv(path_, nrows=1 if head1 else None)
        if path_.suffix.lower() == ".pkl":
            return pd.read_pickle(path_)
    return None


class BrowseState:
    open_files: List[str] = []
    actions: List[Action] = []
    init_vars: Optional[List[List[str]]] = None

    def __init__(self, open_files, actions, init_vars=None):
        self.open_files = open_files
        self.actions = actions
        self.init_vars = init_vars

        self.state = self.get_state()
        super().__init__()

    def get_state(self) -> State:
        if self.init_vars is None:
            self.init_vars = []
            for x in self.open_files:
                y = load_data(x, head1=True)
                if y:
                    self.init_vars.append(list(y.columns))

        state_0 = State(
            Vars=tuple([frozenset(x) for x in self.init_vars]), Tasks=tuple()
        )
        state_n = apply_many_actions(state_0, self.actions)
        return state_n

    def real_outputs(self) -> Iterable[pd.DataFrame]:
        load: List[pd.DataFrame] = [load_data(x) for x in self.open_files if x]

        return perform_actions(load, actions=self.actions)

    def further_actions(self) -> Iterable[Action]:

        return actions_given_state(self.state)

    def get_url_q(self) -> str:
        cons: Dict[str, str] = {}

        for i, act in enumerate(self.actions):
            key_act = f"act_{i}"
            cons[f"{key_act}_task"] = act.Task
            for (i, x), (y, v) in act.CallMap.items():
                cons[f"{key_act}_cm_{i}_{x}"] = f"{y}.{v.q_enc()}"

            for i, (j, x) in enumerate(act.Returns):
                js = "" if j is None else j
                cons[f"{key_act}_ret_{i}"] = f"{js}.{x}"

        for i, opf in enumerate(self.open_files):
            cons[f"file_{i}"] = opf
        return urllib.parse.urlencode(cons)

    @staticmethod
    def from_url_q(x: str) -> "BrowseState":
        parsed = urllib.parse.parse_qs(x)

        max_acts = -1
        for xk in parsed.keys():
            xk = re.sub(r"\Aact_(\d+)_task\Z", r"\1", xk)
            try:
                k_ = int(xk)
            except ValueError:
                continue
            max_acts = max(max_acts, k_)

        actions = []
        for i in range(max_acts + 1):
            task = parsed[f"act_{i}_task"][0]
            cm = {}
            for k, v_ in parsed.items():
                m = re.match(fr"act_{i}_cm_(\d+)_(.*)", k)
                v = v_[0]
                if m:
                    k1, k2 = m.groups()
                    v1, v2 = v.split(".", maxsplit=1)
                    cm[int(k1), k2] = v1, Variable.from_q(v2)
            rets: List[RetArg] = []
            for j in range(len(parsed)):
                try:
                    v1_, v2 = parsed[f"act_{i}_ret_{j}"][0].split(".", maxsplit=1)
                    if v1_:
                        rets.append((int(v1_), v2))
                    else:
                        rets.append((None, v2))
                except KeyError:
                    break
            act = Action(Task=task, CallMap=cm, Returns=rets)
            actions.append(act)

        open_files = []
        for i in range(len(parsed)):
            try:
                open_files.append(parsed[f"file_{i}"])
            except KeyError:
                break

        return BrowseState(open_files=open_files, actions=actions)

    def action_pop_url(self) -> str:

        bs2 = BrowseState(self.open_files, self.actions[:-1], init_vars=self.init_vars)
        return bs2.get_url_q()

