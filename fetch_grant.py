#!/bin/env python3

import os
import time
import argparse

parser = argparse.ArgumentParser(description='fetch patent grants from USPTO bulk data')
parser.add_argument('--files', type=str, default='meta/grant_files.txt', help='list of grant files to fetch')
parser.add_argument('--output', type=str, default='data/grant', help='directory to store fetched files')
parser.add_argument('--delay', type=int, default=10, help='number of seconds to wait between files')
parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')
args = parser.parse_args()

grant_url_fmt = 'https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/{}/{}'

if not os.path.exists(args.output):
    os.makedirs(args.output)

url_list = []
for line in open(args.files):
    line = line.strip()
    path = os.path.join(args.output, line)
    if not args.overwrite and os.path.isfile(path):
        continue

    if line.startswith('ipgb'):
        year = line[4:8]
    elif line.startswith('pgb'):
        year = line[3:7]
    else:
        year = line[0:4]

    url = grant_url_fmt.format(year, line)
    url_list.append((line, path, url))

for name, path, url in sorted(url_list):
    print(f'Fetching {name}')
    os.system(f'curl -o {path} {url}')
    print()
    time.sleep(args.delay)

# to extract:
# cd data/grant
# ls *.zip | xargs -n 1 unzip -n
