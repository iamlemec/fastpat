import os
import time

def fetch_file(zurl, output, overwrite=False, dryrun=False, unzip=False):
    system = print if dryrun else os.system
    zflags = '' if overwrite else '-n'

    if not dryrun and not os.path.exists(output):
        print(f'Creating directory {output}')
        os.makedirs(output)

    _, zname = os.path.split(zurl)
    zpath = os.path.join(output, zname)
    fetch = overwrite or not os.path.isfile(zpath)

    if fetch:
        print(f'Fetching {zname}')
        system(f'curl -o {zpath} {zurl}')

    if fetch or unzip:
        print(f'Unzipping {zname}')
        system(f'unzip {zflags} {zpath} -d {output}')

    return fetch

def fetch_many(files, output, delay=10, dryrun=False, **kwargs):
    for zurl in files:
        fetch = fetch_file(zurl, output, dryrun=dryrun, **kwargs)
        if fetch and not dryrun:
            time.sleep(delay)
