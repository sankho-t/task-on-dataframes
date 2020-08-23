import pandas as pd

import frame_tasks as tada
from frame_tasks import task_view_app, executor


@tada.new_task()
@tada.makes(["mydata.multiline"], appends=False)
@tada.close_task()
def get_mydata(expects, requires):
    return pd.Series(
        ["some sample text", "other text\nend of file"], name=expects[0][1]
    ).to_frame()

