import io
from datetime import datetime
from time import sleep
from math import ceil
from typing import List, Optional

import pandas as pd
import pkg_resources
from flask import (
    abort,
    Flask,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from jinja2 import ChoiceLoader, Environment, FileSystemLoader
from pandas.io.formats.style import Styler

from .serve import app, has_completed, execute_tada_task
from .browse import BrowseState

TEMPLATE_DIR = pkg_resources.resource_filename(__name__, "templates")

VIEW_MAX_COLWIDTH = 30
PAGE_SIZE = 30
NAV_PAGE_COUNT = 5


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


def page_nav(cur: int, maxpage: int) -> List[int]:
    start = max(cur - NAV_PAGE_COUNT // 2, 1)
    stop = min(cur + NAV_PAGE_COUNT // 2, maxpage)
    out = list(range(start, stop + 1))
    if 0 not in out:
        out.insert(0, 0)
    if maxpage not in out:
        out.append(maxpage)
    return out


@app.route("/view/increase_col_width/<int:x>")
def increase_colw(x: int):
    if isinstance(x, str):
        x = int(x)
    y_ = request.cookies.get("colw", VIEW_MAX_COLWIDTH)
    try:
        y = int(y_)
    except ValueError:
        y = VIEW_MAX_COLWIDTH
    y += x
    resp = make_response(str(y))
    resp.set_cookie("colw", str(y))
    return resp


@app.route("/view/decrease_col_width/<int:x>")
def decrease_colw(x: int):
    return increase_colw(-1 * x)


@app.route("/view/<int:page>/<int:index>/<string:q>")
def view(index: int, page: int, q: str):

    if isinstance(index, str):
        index = int(index)
    if isinstance(page, str):
        if page.lower() == "first":
            page = 0
        elif page.lower() == "last":
            page = -1
        page = int(page)

    hc = has_completed(q)
    if isinstance(hc, list):
        out_ = hc[::-1]
    elif hc == None:
        execute_tada_task.delay(q)
        sleep(3)
        return view(index, page, q)
    elif isinstance(hc, datetime):
        resp = make_response(
            render_template(
                "data_wait.html", start_time=hc.strftime("%H:%M:%S %d %b %Y")
            )
        )
        resp.headers["Cache-Control"] = "no-cache, revalidate"
        resp.headers["Expires"] = "0"
        return resp
    else:
        abort(404)
    index = min(len(out_), index)
    out = out_[index]

    npages = ceil(out.shape[0] / PAGE_SIZE)

    if page < 0:
        page = npages + page
    out = out.head(PAGE_SIZE * (page + 1)).tail(PAGE_SIZE)
    nav_pages_name = [
        {0: "First", npages - 1: "Last"}.get(x, str(x))
        for x in page_nav(page, maxpage=npages - 1)
    ]

    nav_pages = dict(
        zip(
            nav_pages_name,
            [
                url_for("view", index=index, page=x, q=q)
                for x in page_nav(page, maxpage=npages - 1)
            ],
        )
    )
    try:
        current_page: Optional[int] = page_nav(page, maxpage=npages - 1).index(page)
    except IndexError:
        current_page = None

    escape_html_tags = (
        lambda x: x.replace("<", r"\<").replace(">", r"\>") if isinstance(x, str) else x
    )
    trim_col = (
        lambda x: x[:VIEW_MAX_COLWIDTH] + "..." if len(x) > VIEW_MAX_COLWIDTH else x
    )

    data = pd.DataFrame(out).applymap(escape_html_tags).astype(str).applymap(trim_col)

    table = MyStyler(data).render()

    col_resize = [url_for("increase_colw", x=10), url_for("decrease_colw", x=10)]

    back_url = url_for("explore", q=q)
    navs = {
        "Back": back_url,
        "Download as csv": url_for("download_csv", index=index, q=q),
    }
    return render_template(
        "viewdata.html",
        table=table,
        navs=navs,
        nav_pages=nav_pages,
        current_page=current_page,
        col_resize=col_resize,
    )


@app.route("/download/csv/<index>/<q>")
def download_csv(index: int, q: str):
    bs = BrowseState.from_url_q(q)

    if isinstance(index, str):
        index = int(index)

    out: pd.DataFrame = list(bs.real_outputs())[::-1][index]

    ret = io.BytesIO(out.to_csv().encode("utf-8"))
    name = max(out.columns, key=len)
    return send_file(
        ret, mimetype="text/csv", as_attachment=True, attachment_filename=name
    )
