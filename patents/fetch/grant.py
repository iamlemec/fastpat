#!/bin/env python3

import os
import time
import argparse

parser = argparse.ArgumentParser(description='fetch patent grants from USPTO bulk data')
parser.add_argument('--files', type=str, default='meta/grant_files.txt', help='list of grant files to fetch')
parser.add_argument('--output', type=str, default='data/grant', help='directory to store fetched files')
parser.add_argument('--delay', type=int, default=10, help='number of seconds to wait between files')
parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')
parser.add_argument('--dryrun', action='store_true', help='just print commands that would be run')
args = parser.parse_args()

grant_url_fmt = 'https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/{}/{}'

def get_base(base):
    return zbase.split('_')[0]

if args.dryrun:
    system = print
    delay = 0
else:
    system = os.system
    delay = args.delay

if not os.path.exists(args.output):
    os.makedirs(args.output)

for line in open(args.files):
    zname = line.strip()
    zbase, _ = os.path.splitext(zname)
    base = get_base(zbase)

    if base.startswith('ipgb'):
        year = base[4:8]
        xname = f'{base}.xml'
    elif base.startswith('pgb'):
        year = base[3:7]
        xname = f'{base}.xml'
    else:
        year = base[0:4]
        xname = f'{base}.dat'

    zpath = os.path.join(args.output, zname)
    xpath = os.path.join(args.output, xname)
    zurl = grant_url_fmt.format(year, zname)

    if args.overwrite or not os.path.isfile(zpath):
        print(f'Fetching {zname}')
        system(f'curl -o {zpath} {zurl}')
        time.sleep(delay)

    if args.overwrite or not os.path.isfile(xpath):
        print(f'Unzipping {zname}')
        system(f'unzip {zpath} -d {args.output}')
