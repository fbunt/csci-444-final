import argparse
from dask.diagnostics import ProgressBar
import os
import pandas as pd
import pendulum as pdm
import xarray as xr

from util import get_year_dirs, LAT_KEY, LON_KEY, SST_KEY, TIME_KEY, xdt_to_dt


_DELIM = ","


def _calc_parallel(task, task_name):
    """Runs a parallel dask operation"""
    print(f"Running: {task_name}")
    with ProgressBar():
        return task.values


def extract_and_write_stats(year_dirs, fd, skip):
    for yd in year_dirs:
        yi = int(os.path.basename(yd))
        if yi in skip:
            print(f"Skipping: {yi}")
            continue
        print("Extracting stats for {}".format(os.path.basename(yd)))
        ds = xr.open_mfdataset(os.path.join(yd, "*.nc"), parallel=True)
        time = ds[TIME_KEY]
        sst = ds[SST_KEY]
        dims = [LAT_KEY, LON_KEY]

        # Calc values in parallel
        vdates = [xdt_to_dt(t) for t in time]
        vmins = _calc_parallel(sst.min(dim=dims), "MIN")
        vmaxes = _calc_parallel(sst.max(dim=dims), "MAX")
        vmeans = _calc_parallel(sst.mean(dim=dims), "MEAN")
        print("")

        for values in zip(vdates, vmins, vmaxes, vmeans):
            _write_line(fd, *values)


def _write_line(fd, *values):
    # pendulum uses RFC 3339 when printed. I wish everything did... :(
    line = _DELIM.join(["{}".format(v) for v in values]) + "\n"
    fd.write(line)


class _Year:
    def __init__(self, year):
        self.year = year
        self._total_days = 365 + pdm.date(year, 1, 1).is_leap_year()
        self.values = []

    def add(self, dt, values):
        if dt.year != self.year:
            raise ValueError("Attempted to add date with wrong year")
        self.values.append((dt, *values))

    def is_complete(self):
        # 8 samples per day
        return len(self.values) == (self._total_days * 8)


def recover_data(fd):
    """Attempts to recover data from `fd`."""
    data = pd.read_csv(fd)
    times = [pdm.parse(dstr) for dstr in data[HEADERS[0]].values]
    vkeys = [k for k in data.columns if k != HEADERS[0]]
    values = zip(times, *[data[k].values for k in vkeys])
    years = []
    last_year = -1
    for dt, *vals in values:
        if dt.year > last_year:
            years.append(_Year(dt.year))
        years[-1].add(dt, vals)
        last_year = dt.year
    whole_years = [y for y in years if y.is_complete()]
    # Years to skip on reprocessing data files
    skip_years = [y.year for y in whole_years]
    out_data = []
    for y in whole_years:
        out_data.extend(y.values)
    return frozenset(skip_years), out_data


def _validate_data_dir(d):
    path = os.path.abspath(d)
    if not os.path.isdir(path):
        raise ValueError("Invalid data dir")
    return path


def _validate_read_file(f):
    if not os.path.isfile(f):
        raise ValueError("Invalid data file")
    return os.path.abspath(f)


def _get_parser():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-d", "--data-dir", type=_validate_data_dir, help="Root data directory"
    )
    p.add_argument(
        "-o", "--out-file", type=os.path.abspath, help="Output file path"
    )
    p.add_argument(
        "-r",
        "--recover",
        type=_validate_read_file,
        default="",
        help="Attempt to recover data from the specified file",
    )
    return p


HEADERS = ["#UTC", "min", "max", "mean"]


if __name__ == "__main__":
    # WARNING: This program takes a while
    args = _get_parser().parse_args()
    year_dirs = get_year_dirs(args.data_dir)

    skip_years = frozenset()
    rec_data = None
    if args.recover:
        print(f"Recovering data from `{args.recover}`")
        with open(args.recover) as fd:
            skip_years, rec_data = recover_data(fd)
        print("Recovered data for the following years:")
        for y in skip_years:
            print(y)

    with open(args.out_file, "w") as fd:
        _write_line(fd, "#UTC", "min", "max", "mean")
        if args.recover:
            for values in rec_data:
                _write_line(fd, *values)
        extract_and_write_stats(year_dirs, fd, skip_years)
