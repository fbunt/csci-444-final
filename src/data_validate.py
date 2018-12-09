"""
This module is needed because the NOAA website has some issues with their data
archive. In particular, the 2012 data is troublesome because the headers for
those files have somehow ended up masquerading as regular files in several
other years.
"""
import glob
import pendulum as pdm
import os

from util import FILE_DATE_RE, get_data_dir_arg_parser, get_year_dirs


def check_for_out_of_place_files(dir, year):
    dir = os.path.abspath(dir)
    print(f"Checking {year}")
    files = glob.glob(os.path.join(dir, "*"))
    oop_files = []
    for f in files:
        if not f.endswith(".nc"):
            oop_files.append(f)
            continue
        fname = os.path.basename(f)
        match = FILE_DATE_RE.match(fname)
        if match is None:
            oop_files.append(f)
            continue
        date = pdm.date(*[int(v) for v in match.groups()])
        if date.year != year:
            oop_files.append(f)
            continue
    return oop_files


def remove_out_of_place_files(data_dir):
    print("Preparing to remove out of place files")
    year_dirs = get_year_dirs(data_dir)
    years = [int(os.path.basename(d)) for d in year_dirs]
    for y, yd in zip(years, year_dirs):
        oop_files = check_for_out_of_place_files(yd, y)
        for f in oop_files:
            print(f"Removing {f}")
            os.remove(f)


def remove_bad_size_files(data_dir):
    pass


def _get_parser():
    p = get_data_dir_arg_parser()
    p.add_argument(
        "-o",
        "--oop",
        action="store_true",
        help="Find and remove out of place files",
    )
    p.add_argument(
        "-s",
        "--size",
        action="store_true",
        help="Find and remove files with incorrect size. Uses HTTP requests",
    )
    return p


if __name__ == "__main__":
    args = _get_parser().parse_args()
    print("Validating")
    if args.oop:
        print("Checking for out of place files")
        remove_out_of_place_files(args.data_dir)
