import os
import time
from urllib.request import urlretrieve
from zipfile import ZipFile

def fetch_file(zurl, output, overwrite=False, dryrun=False, unzip=False):
    system = print if dryrun else os.system

    if not dryrun and not os.path.exists(output):
        print(f'Creating directory {output}')
        os.makedirs(output)

    _, zname = os.path.split(zurl)
    zpath = os.path.join(output, zname)
    fetch = overwrite or not os.path.isfile(zpath)

    if fetch:
        print(f'Fetching {zname}')
        try:
            urlretrieve(zurl, zpath)
        except:
            print(f'Failed to fetch {zurl}')
            return True

    if fetch or unzip:
        print(f'Unzipping {zname}')
        with ZipFile(zpath, 'r') as zfile:
            zfile.extractall(output)

    return fetch

def fetch_many(files, output, delay=10, dryrun=False, **kwargs):
    for zurl in files:
        fetch = fetch_file(zurl, output, dryrun=dryrun, **kwargs)
        if fetch and not dryrun:
            time.sleep(delay)
