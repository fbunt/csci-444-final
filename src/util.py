import argparse
import glob
import os
import pendulum as pdm
import pickle
import re
from sys import stdout


_YEAR_DIR_RE = re.compile(".*/\\d{4}$")
# Picks out the date in the file name
FILE_DATE_RE = re.compile(
    "SEAFLUX-OSB-CDR_V02R00_SST_D(\\d{4})(\\d{2})(\\d{2})_C\\d{8}.nc"
)


def _is_year_dir(path):
    return _YEAR_DIR_RE.match(path) is not None


def get_year_dirs(datadir):
    root = os.path.abspath(datadir)
    year_dirs = glob.glob(root + "/*")
    return [os.path.abspath(d) for d in filter(_is_year_dir, year_dirs)]


def get_data_file_names(datadir):
    year_dirs = get_year_dirs(datadir)
    years = [os.path.basename(d) for d in year_dirs]
    out = {}
    for i, y in years:
        yroot = year_dirs[i]
        yfiles = glob.glob(yroot + "/*.nc")
        yfiles.sort()
        out[y] = yfiles
    return out


LAT_KEY = "lat"
LON_KEY = "lon"
TIME_KEY = "time"
SST_KEY = "sea_surface_temperature"


def xextr(xvalue):
    """Convenience func for extracting a single value from an xarray value."""
    return xvalue.values.item()


# Seconds in nanosecond
_NS = 1e-9


def xdt_to_dt(xdt):
    """Convert the xarray representation (np.datetime64) to a useful type.

    np.datetime64 objects are nice but difficult to work with so convert them
    to pendulum datetimes.
    """
    return pdm.from_timestamp(xextr(xdt) * _NS, "UTC")


class ProgressIndicator:
    """Prints out an indication of progress."""

    def __init__(self, min_=0, max_=100, width=50, fill="#", unit="Bytes"):
        """
        If max > min then a progress bar will be displayed. Otherwise, the
        update value will simply be printed.
        """
        self._fill = str(fill)
        self._width = width
        self._fmt = "\r[{:-<" + str(width) + "}] {:3.1%}"
        self.min = min_
        self.max = max_
        self.range = self.max - self.min
        self._unit = unit
        self.value = 0.0
        # Display progress bar
        self._mode = "BAR"
        if self.max <= self.min:
            # Just display the update value
            self._mode = "VALUE"

    def _get_progress(self):
        p = 0.0
        if self.value >= self.min:
            p = self.value / self.range
        if p > 1.0:
            p = 1.0
        return p

    def update(self, value):
        # NOTE: will overwrite current line on screen. clear space for it
        self.value = value
        if self._mode == "BAR":
            progress = self._get_progress()
            nticks = int(progress * self._width)
            stdout.write(self._fmt.format(self._fill * nticks, progress))
        else:
            # Overwrite last print
            stdout.write("\r{:100}".format(" "))
            stdout.write("\r{} {}".format(value, self._unit))
        stdout.flush()

    def done(self):
        print("")


def cache_data(data, dest_path, force=False):
    """Cache data to the specified destination using pickle.

    Use force to overwrite.
    """
    dest_path = os.path.abspath(dest_path)
    dest_dir = os.path.dirname(dest_path)
    if not os.path.isdir(dest_dir):
        print(f"Creating cache dir: {dest_dir}")
        os.makedirs(dest_dir)
    print(f"Caching data to {dest_path}")
    if os.path.isfile(dest_path):
        if not force:
            print("Cache already exists. Aborting")
        else:
            print("Overwriting cached file")
    with open(dest_path, "wb") as fd:
        pickle.dump(data, fd, protocol=pickle.HIGHEST_PROTOCOL)


def load_cached_data(data_path):
    data_path = os.path.abspath(data_path)
    if not os.path.isfile(data_path):
        print(f"No such cache file: {data_path}")
        return None
    with open(data_path, "rb") as fd:
        return pickle.load(fd)


def get_data_dir_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-d",
        "--data-dir",
        default="../data",
        help="Data directory",
    )
    return p
