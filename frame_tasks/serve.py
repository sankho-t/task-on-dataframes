from .basic_tasks import tada
from .browse import BrowseState

from flask import Flask, render_template, url_for

app = Flask(__name__)

print(tada.tasks.keys())


@app.route("/explore")
@app.route("/explore/<q>")
def explore(q=""):
    bs = BrowseState.from_url_q(q)

    bs.vars = bs.state.Vars

    act_links_q = []
    more_acts = bs.further_actions()
    for act in more_acts:
        bs.actions.append(act)
        url = url_for("explore", q=bs.get_url_q())
        act_links_q.append(url)
        bs.actions.pop()

    back_url = url_for("explore", q=bs.action_pop_url())

    return render_template(
        "action.html",
        current_state=bs,
        future_actions=more_acts,
        future_act_links=act_links_q,
        back=back_url,
    )

