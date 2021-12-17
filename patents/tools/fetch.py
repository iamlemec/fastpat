#!/bin/env python3

import os
import time

def fetch_file(zurl, output, overwrite=False, dryrun=False):
    system = print if dryrun else os.system
    zflags = '' if overwrite else '-n'

    if not os.path.exists(output):
        os.makedirs(output)

    _, zname = os.path.split(zurl)
    zpath = os.path.join(output, zname)

    if overwrite or not os.path.isfile(zpath):
        print(f'Fetching {zname}')
        system(f'curl -o {zpath} {zurl}')

    print(f'Unzipping {zname}')
    system(f'unzip {zflags} {zpath} -d {output}')

def fetch_many(files, output, delay=10, dryrun=False, **kwargs):
    for zurl in files:
        fetch_file(zurl, output, dryrun=dryrun, **kwargs)
        if not dryrun:
            time.sleep(delay)
