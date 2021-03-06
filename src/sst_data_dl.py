#!~/anaconda3/bin/python3
import argparse
from bs4 import BeautifulSoup as BSoup, SoupStrainer
import os
import pendulum as pdm
import re
import requests
import time
import traceback
from urllib.request import urljoin, urlopen

from util import cache_data, FILE_DATE_RE, load_cached_data, ProgressIndicator


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


def _validate_furl(url, year):
    fname = os.path.basename(url)
    date_match = FILE_DATE_RE.match(fname)
    date = pdm.date(*[int(v) for v in date_match.groups()])
    if date.year != int(year):
        return False
    return True


def get_data_file_urls(base_url):
    years = _scrape_years(base_url)
    out = {}
    for y in years:
        url = urljoin(base_url, y + "/")
        file_names = _scrape_data_file_names(url)
        df_urls = [urljoin(url, fi) for fi in file_names]
        df_urls = [url for url in df_urls if _validate_furl(url, y)]
        out[y] = df_urls
    return out


CACHE_DIR = "../cache"
LINKS_CACHE_FILE = "targets.p"


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
        self.total_files = 0
        self.files_downloaded = 0
        self.files_touched = 0

    def run(self, use_cache=True):
        self._get_targets_info(use_cache)
        self._dl_targets()
        print(f"Files downloaded: {self.files_downloaded}/{self.total_files}")
        print(f"Files touched: {self.files_touched}/{self.total_files}")
        print("Total data: {}".format(_get_size_str(self.total_bytes)))

    def _get_targets_info(self, use_cache):
        print(f"Scraping targest from {self._base_url}")
        targets = None
        write_cache = True
        if use_cache:
            targets = load_cached_data(self._target_cache_path)
            if targets is None:
                targets = get_data_file_urls(self._base_url)
                write_cache = False
            else:
                print("Targets loaded from cache")
        cache_data(targets, self._target_cache_path, force=write_cache)
        self._targets = targets
        self._years = list(self._targets.keys())
        self._years.sort()
        count = 0
        for k in self._targets:
            count += len(self._targets[k])
        print(f"Found {count} data files")
        self.total_files = count

    def _dl_targets(self):
        print("\nStarting downloads\n")
        fnum = 0
        for y in self._years:
            print(f"Downloading year: {y}")
            dest_dir = os.path.join(self._dest_dir, y)
            os.makedirs(dest_dir, exist_ok=True)
            for target_url in self._targets[y]:
                fnum += 1
                self._dl_target_file(dest_dir, target_url, fnum)
                time.sleep(self._time_buffer)

    def _dl_target_file(self, dest_dir, target_url, fnum):
        f = os.path.basename(target_url)
        dest = os.path.join(dest_dir, f)
        # Assume file names already validated
        date_match = FILE_DATE_RE.match(f)
        date = pdm.date(*[int(v) for v in date_match.groups()])
        print(f"File {fnum}/{self.total_files}")
        print(date)
        print(f"Downloading: {target_url}")
        print(f"Destination: {dest}")
        if os.path.isfile(dest):
            self.files_touched += 1
            print("File already downloaded. Skipping\n")
            return
        with requests.get(target_url, stream=True) as r:
            try:
                self.total_bytes += _dl_file(r, dest)
                print("")
                self.files_downloaded += 1
                self.files_touched += 1
            except Exception:
                # This does not catch KeyboardInterupt
                print("")
                print("Encountered error when attempting to download file:")
                print(f"File: {target_url}")
                traceback.print_exc()


def _dl_file(req, dest):
    """Downloads the file pointed to by `req` to `dest`.

    The file is downloaded to a temporary file and then moved to the
    destination if the download is successful. If the download fails or is
    interrupted, the temporary file is removed. This makes validation easy.
    """
    bytes_ = 0
    tmp_dest = dest + "_tmp"
    fd = open(tmp_dest, "wb")
    size = -1
    try:
        size = int(req.headers["Content-Length"])
    except KeyError:
        pass
    size_str = _get_size_str(size)
    if not size_str:
        raise IOError(f"File too large: {size}")
    print(f"Downloading: {size_str}")
    prog = ProgressIndicator(0, size)
    finished = False
    try:
        for chunk in req.iter_content(chunk_size=(5 * 1024 * 1024)):
            if chunk:
                bytes_ += len(chunk)
                fd.write(chunk)
                prog.update(bytes_)
        finished = True
    finally:
        fd.close()
        if finished:
            os.rename(tmp_dest, dest)
        else:
            os.remove(tmp_dest)
        prog.done()
    print("Done")
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
        BASE_URL,
        args.data_dir,
        time_buffer=0.001,
        target_cache_dir=args.cache_dir,
    )
    dloader.run(not args.clear_cache)
