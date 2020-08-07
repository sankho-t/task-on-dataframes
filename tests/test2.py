import re

import pandas as pd
import frame_tasks as tada

any_name = re.compile(r"(.+)")


@tada.new_task()
@tada.requires([any_name], arg="x")
@tada.makes([r"{x.0.0}.alpha"], appends=True)
@tada.close_task()
def remove_num(x: pd.DataFrame, requires, expects) -> pd.DataFrame:
    data = x[requires[("x", any_name)]]
    data.update(data.str.replace(r"\d", ""))
    data.name = expects[0][1]
    return data.to_frame()


@tada.new_task()
@tada.requires([any_name], arg="x")
@tada.makes([r"{x.0.0}.split"], appends=True)
@tada.close_task()
def splitter(x: pd.DataFrame, requires, expects) -> pd.DataFrame:
    data = x[requires[("x", any_name)]].str.split(" ").explode()
    data.name = expects[0][1]
    return data.to_frame()


x = pd.DataFrame([["sankho123 turjo sarkar456"]], columns=["name"]).reset_index()

results = tada.Executor([x.copy()], [["name.split.alpha"]])
for ret in reversed(results):
    print(ret)
