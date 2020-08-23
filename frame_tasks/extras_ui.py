from palettable.colorbrewer.diverging import RdGy_10 as colormap


def get_unique_colors(n):
    cm = colormap.mpl_colormap
    out = []
    step = 1.0 / n
    to_hex = lambda x: "{:0>2}".format(hex(int(x))[2:])
    for i in range(n):
        col = cm(i * step)
        out.append("#" + "".join(to_hex(col[j] * 256) for j in range(3)))
    return out
