import argparse
import os
import pendulum as pdm
import xarray as xr

from util import (
    get_year_dirs,
    ProgressIndicator,
    SST_KEY,
    TIME_KEY,
    xdt_to_dt,
    xextr,
)


def extract_and_write_stats(year_dirs, fd):
    for yd in year_dirs:
        print("Extracting extremes for {}".format(os.path.basename(yd)))
        ds = xr.open_mfdataset(year_dir + "/*.nc")
        time = ds[TIME_KEY]
        sst = ds[SST_KEY]
        prog = ProgressIndicator(0, len(time), width=70)
        prog.update(0)
        for i, t in enumerate(time):
            pdt = xdt_to_dt(t)
            ssti = sst.sel(time=t)
            min_ = xextr(ssti.min())
            max_ = xextr(ssti.max())
            mean = xextr(ssti.mean())
            _write_line(fd, pdt, min_, max_, mean)
            prog.update(i + 1)
        prog.done()


def _write_line(fd, *values):
    # pendulum uses RFC 3339 when printed. I wish everything did... :(
    line = ",".join(["{}".format(v) for v in values]) + "\n"
    fd.write(line)


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
    with open(args.out_file) as fd:
        extract_and_write_stats(year_dirs, fd)
