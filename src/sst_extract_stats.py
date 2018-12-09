import argparse
import os
# Progress bar lib
from tqdm import tqdm
import xarray as xr

from util import (
    get_year_dirs,
    SST_KEY,
    TIME_KEY,
    xdt_to_dt,
    xextr,
)


def extract_and_write_stats(year_dirs, fd):
    for yd in year_dirs:
        print("Extracting stats for {}".format(os.path.basename(yd)))
        ds = xr.open_mfdataset(os.path.join(yd, "*.nc"))
        time = ds[TIME_KEY]
        sst = ds[SST_KEY]
        for i, t in enumerate(tqdm(time)):
            pdt = xdt_to_dt(t)
            ssti = sst.sel(time=t)
            min_ = xextr(ssti.min())
            max_ = xextr(ssti.max())
            mean = xextr(ssti.mean())
            _write_line(fd, pdt, min_, max_, mean)


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
    with open(args.out_file, "w") as fd:
        _write_line(fd, "#UTC", "min", "max", "mean")
        extract_and_write_stats(year_dirs, fd)
