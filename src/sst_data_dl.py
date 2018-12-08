#!~/anaconda3/bin/python3
import argparse
from bs4 import BeautifulSoup as BSoup, SoupStrainer
import os
import pickle
import re
import requests
from sys import stdout
import time
import traceback
from urllib.request import urljoin, urlopen

BASE_URL = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-whoi/access/"
)

YEAR_RE = re.compile("\\d{4}/")


def _is_year(s):
    return YEAR_RE.match(s) is not None


def _is_data_file(s):
    return s.startswith("SEAFLUX-OSB-CDR") and s.endswith(".nc")


def _scrape_links(url, filter_func=None):
    print(f"Scraping for links: {url}")
    filter_func = filter_func or (lambda x: True)
    a_tags = SoupStrainer("a")
    soup = BSoup(urlopen(url), "lxml", parse_only=a_tags)
    links = [a.get("href") for a in soup.find_all("a")]
    return list(filter(filter_func, links))


def _scrape_years(url):
    ylinks = _scrape_links(url, _is_year)
    years = [y[:4] for y in ylinks]
    return years


def _scrape_data_file_names(url):
    dflinks = _scrape_links(url, _is_data_file)
    dflinks.sort()
    return dflinks


def get_data_file_urls(base_url):
    years = _scrape_years(base_url)
    out = {}
    for y in years:
        url = urljoin(base_url, y + "/")
        file_names = _scrape_data_file_names(url)
        df_urls = [urljoin(url, fi) for fi in file_names]
        out[y] = df_urls
    return out


class _ProgressIndicator:
    """Prints out an indication of progress."""

    def __init__(self, min_=0, max_=100, width=50, fill="#", unit="Bytes"):
        """Return new ProgressIndicator.

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


CACHE_DIR = "../cache"
LINKS_CACHE_FILE = "targets.p"


def _cache_data(data, dest_path, force=False):
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


def _load_cached_data(data_path):
    data_path = os.path.abspath(data_path)
    if not os.path.isfile(data_path):
        print(f"No such cache file: {data_path}")
        return None
    with open(data_path, "rb") as fd:
        return pickle.load(fd)


class SSTBulkDownloader:
    """Class that performs a bulk download of NOAA SST data files."""

    def __init__(
        self, base_url, dest_dir, time_buffer=0.1, target_cache_dir=CACHE_DIR
    ):
        self._base_url = base_url
        self._dest_dir = os.path.abspath(dest_dir)
        # delay between consecutive downloads
        self._time_buffer = time_buffer
        cache_path = os.path.join(target_cache_dir, LINKS_CACHE_FILE)
        self._target_cache_path = os.path.abspath(cache_path)
        self._years = []
        self._targets = None
        self.total_bytes = 0
        self.file_count = 0
        self.files_downloaded = 0

    def run(self, use_cache=True):
        self._get_targets_info(use_cache)
        self._dl_targets()
        print(f"Files downloaded: {self.files_downloaded}/{self.file_count}")
        print("Total data: {}".format(_get_size_str(self.total_bytes)))

    def _get_targets_info(self, use_cache):
        print(f"Scraping targest from {self._base_url}")
        targets = None
        write_cache = True
        if use_cache:
            targets = _load_cached_data(self._target_cache_path)
            if targets is None:
                targets = get_data_file_urls(self._base_url)
                write_cache = False
            else:
                print("Targets loaded from cache")
        _cache_data(targets, self._target_cache_path, force=write_cache)
        self._targets = targets
        self._years = list(self._targets.keys())
        self._years.sort()
        count = 0
        for k in self._targets:
            count += len(self._targets[k])
        print(f"Found {count} data files")
        self.file_count = count

    def _dl_targets(self):
        print("\nStarting downloads\n")
        for y in self._years:
            print(f"Downloading year: {y}")
            dest_dir = os.path.join(self._dest_dir, y)
            os.makedirs(dest_dir, exist_ok=True)
            for target_url in self._targets[y]:
                f = os.path.basename(target_url)
                dest = os.path.join(dest_dir, f)
                print(f"Downloading: {target_url}")
                print(f"Destination: {dest}")
                if _validate(dest, target_url):
                    print("File already downloaded. Skipping\n")
                    continue
                try:
                    self.total_bytes += _dl_file(target_url, dest)
                    print("")
                    self.files_downloaded += 1
                except Exception:
                    print("")
                    print(
                        "Encountered error when attempting to download file:"
                    )
                    print(f"File: {target_url}")
                    traceback.print_exc()
                time.sleep(self._time_buffer)


def _validate(dest, url):
    """Check if the local copy of the file at `url` is valid.

    Checks that file sizes match. If the remote file size can't be determined,
    this func plays it safe and claims the local copy isn't valid
    """
    if not os.path.isfile(dest):
        return False
    local_size = os.path.getsize(dest)
    remote_size = -1
    with requests.head(url) as r:
        # Header may not contain size
        try:
            remote_size = int(r.headers["Content-Length"])
        except KeyError:
            pass
    # Err on the side of caution and dl the file if remote size couldn't be
    # determined.
    return local_size == remote_size


def _dl_file(url, dest):
    bytes_ = 0
    with open(dest, "wb") as fd, requests.get(url, stream=True) as r:
        size = -1
        try:
            size = int(r.headers["Content-Length"])
        except KeyError:
            pass
        size_str = _get_size_str(size)
        if not size_str:
            raise IOError(f"File too large: {size}")
        print(f"Downloading: {size_str}")
        prog = _ProgressIndicator(0, size)
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                bytes_ += len(chunk)
                fd.write(chunk)
                prog.update(bytes_)
        print("\nDone")
    return bytes_


_SUFFIXES = ["B", "KB", "MB", "GB", "TB"]
_SIZE_FMT = "{:4.1f} {}"


def _get_size_str(size):
    if size < 0:
        return "Unknown size"
    size_ = size
    for s in _SUFFIXES:
        if size_ >= 1024:
            size_ /= 1024.0
        else:
            return _SIZE_FMT.format(size_, s)
    # If we get out of the loop, the file is toooooo large
    return ""


def _get_parser():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-d",
        "--data-dir",
        default="../data",
        help="Data destination directory",
    )
    p.add_argument(
        "-c",
        "--cache-dir",
        default=CACHE_DIR,
        help="Directory for caching data",
    )
    p.add_argument(
        "-r", "--clear-cache", action="store_true", help="Clear the cache"
    )
    return p


if __name__ == "__main__":
    args = _get_parser().parse_args()
    args.data_dir = os.path.abspath(args.data_dir)
    if not os.path.isdir(args.data_dir):
        print(f"Creating data dir: {args.data_dir}")
        os.makedirs(args.data_dir)
    dloader = SSTBulkDownloader(
        BASE_URL, args.data_dir, target_cache_dir=args.cache_dir
    )
    dloader.run(not args.clear_cache)
