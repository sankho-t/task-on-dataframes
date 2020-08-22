import io
import os
import logging
import pathlib
import pickle as pk
import shutil
import tempfile
from datetime import datetime
from time import sleep
from typing import List, Optional, Union

import mmh3
import pandas as pd
from celery import Celery
from celery.worker.request import Request as celeryRequest
from flask import Flask, make_response, render_template, request, send_file, url_for
from palettable.colorbrewer.diverging import RdGy_10 as colormap

from .basic_tasks import tada
from .browse import BrowseState

app = Flask(__name__)


app.config.update(CELERY_BROKER_URL=os.environ["CELERY_BROKER_URL"],)


def make_celery(app):
    celery = Celery(app.import_name, broker=app.config["CELERY_BROKER_URL"],)
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery


celery = make_celery(app)


def get_unique_colors(n):
    cm = colormap.mpl_colormap
    out = []
    step = 1.0 / n
    to_hex = lambda x: "{:0>2}".format(hex(int(x))[2:])
    for i in range(n):
        col = cm(i * step)
        out.append("#" + "".join(to_hex(col[j] * 256) for j in range(3)))
    return out


logger = logging.getLogger("frame_tasks.celery")


def browsestate_save_path(q: str) -> pathlib.Path:
    hash_ = mmh3.hash(q, 100)
    return pathlib.Path(tempfile.gettempdir()) / f"frame_tasks.{hash_}.pkl"


@celery.task(name="frame_tasks.serve_exec")
def execute_tada_task(browse_state: str):
    path = browsestate_save_path(browse_state)
    logger.info(f"Triggered task {path}")
    if path.exists():
        x = has_completed(browse_state)
        if isinstance(x, (list, datetime)):
            return True
    with open(path, "wb") as f:
        pk.dump(datetime.now(), f)
    logger.info("Started task")
    bs = BrowseState.from_url_q(browse_state)
    try:
        out = list(bs.real_outputs())
    except Exception as exc:
        logger.exception(exc)
        with open(path, "wb") as f:
            pass
        return None
    with open(path, "wb") as f:
        pk.dump(out, f)
    return True


def has_completed(
    browse_state: str,
) -> Optional[Union[List[pd.DataFrame], datetime, bool]]:
    path = browsestate_save_path(browse_state)
    try:
        with open(path, "rb") as f:
            data = pk.load(f)
    except FileNotFoundError:
        return None
    except (IOError, EOFError):
        return None
    except pk.UnpicklingError:
        sleep(5000)
        try:
            with open(path, "rb") as f:
                data = pk.load(f)
        except pk.UnpicklingError:
            return None
        except:
            return has_completed(browse_state)
    if isinstance(data, datetime):
        return data
    if isinstance(data, list):
        if all(lambda x: isinstance(x, pd.DataFrame) for x in data):
            return data
    logger.warn(f"Unknown data format in {path}")
    return False


@app.route("/explore/")
@app.route("/explore/<q>")
def explore(q=""):
    bs = BrowseState.from_url_q(q)

    bs.vars = bs.state.Vars

    dataview_urls = []
    for ind, _ in enumerate(bs.vars):
        u = url_for("view", q=q, index=ind, page=0)
        dataview_urls.append(u)

    act_links_q = []
    more_acts = bs.further_actions()

    cols_use = set([x for act in more_acts for _, x in act.CallMap.keys()])
    cols_colors = {}
    if cols_use:
        colors = get_unique_colors(len(cols_use))
        cols_colors = dict(zip(cols_use, colors))

    for act in more_acts:
        bs.actions.append(act)
        url = url_for("explore", q=bs.get_url_q())
        act_links_q.append(url)
        bs.actions.pop()

    back_url = url_for("explore", q=bs.action_pop_url())

    return render_template(
        "action.tpl",
        current_state=bs,
        future_actions=more_acts,
        future_act_links=act_links_q,
        back=back_url,
        dataview_urls=dataview_urls,
        cols_colors=cols_colors,
    )
