import pandas as pd
import frame_tasks as tada
import frame_tasks.basic_tasks

a = pd.Series(
    ["this is a multiline text\nother line\nmore lines"], name="sample.multiline"
).to_frame()


for res in reversed(
    tada.Executor([], [["usenet.read_file.lines.clean_tokens.top90"]],)
):
    print(res)
    break

for res in reversed(tada.Executor([a], [["sample.lines.clean_tokens.counts"]],)):
    print(res)
    break
