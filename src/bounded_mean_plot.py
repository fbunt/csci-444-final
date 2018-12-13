import argparse
import datetime as pydt
import matplotlib.pyplot as plt
import numpy as np
import os
import pendulum as pdm
import seaborn as sns
import xarray as xr

from mean_plot import init_plotting


MARKER_SIZE = 1
TITLE_FONT_SIZE = 17
OUT_DPI = 200
LINE_WIDTH = 3.0


def _validate_file(p):
    p = os.path.abspath(p)
    if os.path.isfile(p):
        return p
    raise ValueError("Invalid input file")


def _get_parser():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-i", "--infile", type=_validate_file, help="Input data file"
    )
    p.add_argument("-t", "--title", default="", help="Plot title")
    p.add_argument("-l", "--label", default="", help="trend line label")
    p.add_argument(
        "-n", "--window", default=1000, type=int, help="Smoothing window size"
    )
    p.add_argument(
        "-o",
        "--outfile",
        default="",
        help="If specified, the plot will be saved to this path",
    )
    p.add_argument("-s", "--show", action="store_true", help="Show the plot")
    return p


def read_data(fname):
    ds = xr.open_dataset(fname)
    sst = ds.sea_surface_temperature
    times = sst.time.values
    values = sst.values
    # using this because I know it works. np.datetime64 changes too much
    times = [pdm.parse(str(t)) for t in times]
    times = [pydt.datetime(*t.timetuple()[:6]) for t in times]
    return times, values


def get_smoothed(times, values, n=5*365*8):
    kernel = np.ones(n, dtype=float) / n
    smoothed = np.convolve(values, kernel, "valid")
    return times[n//2:-n//2+1], smoothed


def plot(times, values, args):
    ts, vs = get_smoothed(times, values, args.window)
    plt.figure(figsize=(16, 9))
    plt.plot(times, values, ".", ms=MARKER_SIZE, label="Three Hour Means")
    plt.plot(
        ts,
        vs,
        "-",
        lw=LINE_WIDTH,
        label=args.label,
    )
    plt.title(args.title)
    plt.xlabel("Year")
    plt.ylabel("Sea Surface Temperature (Deg. C)")
    sns.despine(top=True, right=True, trim=True)
    legend = plt.legend(loc=4)
    legend.legendHandles[0]._legmarker.set_markersize(8)

    plt.minorticks_on()
    plt.tick_params(axis="y", which="minor", left=False)
    # Hide 1987 tick
    plt.gca().xaxis.get_minor_ticks()[0].set_visible(False)

    if args.outfile:
        plt.savefig(args.outfile, dpi=OUT_DPI)
    if args.show:
        plt.show()
    plt.close()


if __name__ == "__main__":
    args = _get_parser().parse_args()
    init_plotting()
    dates, values = read_data(args.infile)
    plot(dates, values, args)
