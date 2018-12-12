import argparse
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import os
import pendulum as pdm
import seaborn as sns


MARKER_SIZE = 1
FONT_SIZE = 17
OUT_DPI = 150


def read_data(fd):
    ts = []
    mi = []
    ma = []
    me = []
    # Header
    fd.readline()
    for line in fd:
        vals = line.split(",")
        ts.append(pdm.parse(vals[0]))
        mi.append(float(vals[1]))
        ma.append(float(vals[2]))
        me.append(float(vals[3]))
    ts = [dt.datetime(*d.timetuple()[:6]) for d in ts]
    return ts, np.array(mi), np.array(ma), np.array(me)


def init_plotting():
    sns.set()
    sns.set_style("white")
    sns.set_style("ticks")


def make_plot(data, args):
    plt.figure(figsize=(16, 9))
    plt.plot(data[0], data[3], ".", ms=MARKER_SIZE)
    plt.title(args.title, fontsize=FONT_SIZE)
    plt.xlabel("Year")
    plt.ylabel("Sea Surface Temperature (Deg. C)")
    sns.despine(top=True, right=True, trim=True)

    plt.minorticks_on()
    plt.tick_params(axis="y", which="minor", left=False)
    # Hide 1987 tick
    plt.gca().xaxis.get_minor_ticks()[0].set_visible(False)

    if args.outfile:
        plt.savefig(args.outfile, dpi=OUT_DPI)
    if args.show:
        plt.show()
    plt.close()


def _validate_infile(p):
    p = os.path.abspath(p)
    if os.path.isfile(p):
        return p
    raise ValueError("Invalid input data file")


def _get_parser():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-i", "--infile", type=_validate_infile, help="Input data file"
    )
    p.add_argument("-t", "--title", default="", help="Plot title")
    p.add_argument(
        "-o",
        "--outfile",
        default="",
        help="If specified, the plot will be saved to this path",
    )
    p.add_argument("-s", "--show", action="store_true", help="Show the plot")
    return p


if __name__ == "__main__":
    args = _get_parser().parse_args()
    with open(args.infile) as fd:
        data = read_data(fd)
    init_plotting()
    make_plot(data, args)
