import io
import pandas as pd
import pkg_resources
from flask import Flask, render_template, url_for, send_file
from jinja2 import ChoiceLoader, Environment, FileSystemLoader
from pandas.io.formats.style import Styler
from palettable.colorbrewer.diverging import RdGy_10 as colormap

from .basic_tasks import tada
from .browse import BrowseState

app = Flask(__name__)

print(tada.tasks.keys())


def get_unique_colors(n):
    cm = colormap.mpl_colormap
    out = []
    step = 1.0 / n
    for i in range(n):
        col = cm(i * step)
        out.append("#" + "".join(hex(int(col[j] * 256))[2:] for j in range(3)))
    return out


@app.route("/explore/")
@app.route("/explore/<q>")
def explore(q=""):
    bs = BrowseState.from_url_q(q)

    bs.vars = bs.state.Vars

    dataview_urls = []
    for ind, _ in enumerate(bs.vars):
        u = url_for("view", q=q, index=ind)
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


TEMPLATE_DIR = pkg_resources.resource_filename(__name__, "templates")


class MyStyler(Styler):
    env = Environment(
        loader=ChoiceLoader(
            [
                FileSystemLoader(TEMPLATE_DIR),  # contains ours
                Styler.loader,  # the default
            ]
        )
    )
    template = env.get_template("myhtml.tpl")


@app.route("/view/<index>/<q>")
def view(index: int, q: str):
    bs = BrowseState.from_url_q(q)

    if isinstance(index, str):
        index = int(index)
    out = list(bs.real_outputs())[::-1]
    index = min(len(out), index)

    escape_html_tags = (
        lambda x: x.replace("<", r"\<").replace(">", r"\>") if isinstance(x, str) else x
    )
    data = pd.DataFrame(out[index]).applymap(escape_html_tags)

    table = MyStyler(data).render()

    back_url = url_for("explore", q=q)
    navs = {
        "Back": back_url,
        "Download as csv": url_for("download_csv", index=index, q=q),
    }
    return render_template("viewdata.html", table=table, navs=navs)


@app.route("/download/csv/<index>/<q>")
def download_csv(index: int, q: str):
    bs = BrowseState.from_url_q(q)

    if isinstance(index, str):
        index = int(index)

    out = list(bs.real_outputs())[index]

    x = io.BytesIO()
    out.to_csv(x)

    return send_file(x)
