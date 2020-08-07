import pandas as pd
from frame_tasks import *


@new_task()
@requires(["A", "B"], arg="x")
@makes(["C"], appends=True)
@close_task()
def a_maker(x: pd.DataFrame) -> pd.DataFrame:
    x["C"] = x["A"] + x["B"]
    return x


@new_task()
@requires(["C"], arg="x")
@makes(["D"], appends=True)
@close_task()
def b_maker(x: pd.DataFrame) -> pd.DataFrame:
    x["D"] = "hello" + x["C"]
    return x


print(tasks)


x = pd.DataFrame([["sankho", "turjo"], ["bob", ""]], columns=["A", "B"])

cl = Caller(have={"1": ["A", "B"]}, for_task=tasks["a_maker"])

for xx in cl.satisfy():
    print(xx)

# y = make([x], "D")
# print(y)

# help(a_maker)

# x = tasks["a_maker"]

# args = [[("x", "A"), ("x", "B")]]
# for y in x.generates_from(args):
#     print(y)
