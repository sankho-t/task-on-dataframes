import glob
import re
import pathlib

import pandas as pd
import frame_tasks as tada

pat = re.compile
any_name = pat(r"(.+)")


@tada.new_task()
@tada.makes(["usenet.path"], appends=False)
@tada.close_task()
def get_paths(expects, **kwargs):
    return pd.Series(
        list(glob.glob("20_newsgroups/*/*")), name=expects[0][1]
    ).to_frame()


@tada.new_task()
@tada.requires([pat(r"(.*)\.path")], arg="x")
@tada.makes([r"{x}.read_file.multiline"])
@tada.close_task()
def get_text(x, expects, **kwargs):
    inp = x[x.columns[0]]
    out = inp.apply(lambda x: open(x, errors="replace").read())
    out.name = expects[0][1]
    return x.join(out).reset_index(drop=True)


@tada.new_task()
@tada.requires([re.compile(r"(.+)\.multiline")], arg="x")
@tada.makes([r"{x}.lines"])
@tada.close_task()
def get_splits(x, expects, **kwargs):
    inp = x[x.columns[0]]
    out = inp.str.split("\r?\n").explode()
    out.name = expects[0][1]
    return x.join(out).reset_index(drop=True)


@tada.new_task()
@tada.requires([pat(r"(.+\.lines)")], arg="x")
@tada.makes([r"{x}.mail_from"])
@tada.close_task()
def get_from(x, requires, expects):
    inp = x[x.columns[0]]
    out = inp.loc[inp.str.startswith("From:")]
    out.name = expects[0][1]
    return x.join(out).reset_index(drop=True)


@tada.new_task()
@tada.requires([pat(r"(.+\.lines)")], arg="x")
@tada.makes(["sample_ind", r"{x}.tokens"], appends=False)
@tada.close_task()
def tokenize(x, requires, expects):
    inp = x[x.columns[0]]
    out = inp.str.split(r"\b").explode().str.lower()
    out.name = expects[1][1]
    out = out.reset_index()
    cols = list(out.columns)
    cols[0] = "sample_ind"
    out.columns = cols
    return out


@tada.new_task()
@tada.requires(["sample_ind", pat(r"(.+)\.tokens")], arg="x")
@tada.makes(["sample_ind", r"{x}.clean_tokens"], appends=False)
@tada.close_task()
def tokenize_clean(x, requires, expects):
    toks = x.set_index("sample_ind")[x.columns[1]]

    toks.update(toks.str.replace(r"\A\W+|\W+\Z", ""))
    toks = toks.loc[toks.str.len() > 1]
    toks.name = expects[1][1]
    return toks.reset_index()


@tada.new_task()
@tada.requires([pat(r"(.+)")], arg="x")
@tada.makes([r"{x}.counts", r"{x}"], appends=False)
@tada.close_task()
def counts(x, requires, expects):
    counts = x[x.columns[0]].value_counts().reset_index()
    counts.columns = [next(iter(requires.values())), expects[0][1]]
    return counts


@tada.new_task()
@tada.requires([pat(r"(.+\.clean_tokens)\Z")], arg="x")
@tada.requires([r"{x}", r"{x}.counts"], arg="y")
@tada.makes([r"{x}.top90"], appends=False)
@tada.close_task()
def top90(x, y, requires, expects):
    col = y[y.columns[1]]
    sum = col.sum()
    topwords = col.cumsum() < 0.9 * sum
    y.loc[topwords, "istop"] = True
    x = x.join(y.set_index(y.columns[0])["istop"], on=x.columns[0])
    x = x.loc[x["istop"].fillna(False), x.columns[0]]
    x.name = expects[0][1]
    return x.to_frame()
