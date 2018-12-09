import argparse
import os
import pendulum as pdm
import xarray as xr

from util import get_year_dirs, ProgressIndicator, SST_KEY, TIME_KEY, xdt_to_dt, xextr


def get_year_extremes(year_dir):
    ds = xr.open_mfdataset(year_dir + "/*.nc")
    time = ds[TIME_KEY]
    sst = ds[SST_KEY]
    mins = []
    maxes = []
    ptimes = []
    prog = ProgressIndicator(0, len(time), width=70)
    prog.update(0)
    for i, t in enumerate(time):
        ptimes.append(xdt_to_dt(t))
        ssti = sst.sel(time=t)
        mins.append(xextr(ssti.min()))
        maxes.append(xextr(ssti.max()))
        prog.update(i + 1)
    prog.done()
    return ptimes, mins, maxes


def get_extremes(year_dirs):
    ptimes = []
    mins = []
    maxes = []
    for yd in year_dirs:
        print("Extracting extremes for {}".format(os.path.basename(yd)))
        tmm = get_year_extremes(yd)
        ptimes.extend(tmm[0])
        mins.extend(tmm[1])
        maxes.extend(tmm[2])
    return ptimes, mins, maxes


_LINE_FMT = "{},{},{}\n"


def write_extremes(outfile, times, mins, maxes):
    print(f"Writing data to '{outfile}'")
    with open(outfile) as fd:
        fd.write("#" + LINE_FMT.format("UTC", "min", "max"))
        for ti, mi, ma in zip(times, mins, maxes):
            # pendulum uses RFC 3339 when printed. I wish everything did... :(
            fd.write(LINE_FMT.format(tdt, mi, ma))


def _validate_data_dir(d):
    path = os.path.abspath(d)
    if not os.path.isdir(path):
        raise ValueError("Invalid data dir")
    return path


def _get_parser():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-d", "--data-dir", type=_validate_data_dir, help="Root data directory"
    )
    p.add_argument(
        "-o", "--out-file", type=os.path.abspath, help="Output file path"
    )
    return p


if __name__ == "__main__":
    args = _get_parser().parse_args()
    year_dirs = get_year_dirs(args.data_dir)
    ptimes, mins, maxes = get_extremes(year_dirs)
    write_extremes(args.out_file, ptimes, mins, maxes)
