#!/bin/env python3

import os
import time
import argparse

parser = argparse.ArgumentParser(description='fetch patent assignments from USPTO bulk data')
parser.add_argument('--files', type=str, default='meta/assign_files.txt', help='list of assignment files to fetch')
parser.add_argument('--output', type=str, default='data/assign', help='directory to store fetched files')
parser.add_argument('--delay', type=int, default=1, help='number of seconds to wait between files')
parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')
args = parser.parse_args()

assign_url_fmt = 'https://bulkdata.uspto.gov/data/patent/assignment/{}'

if not os.path.exists(args.output):
    os.mkdir(args.output)

url_list = []
for line in open(args.files):
    line = line.strip()
    path = os.path.join(args.output, line)
    if not args.overwrite and os.path.isfile(path):
        continue

    year = int(line[2:6])

    url = assign_url_fmt.format(line)
    url_list.append((line, path, url))

for name, path, url in sorted(url_list):
    print(f'Fetching {name}')
    os.system(f'curl -o {path} {url}')
    print()
    time.sleep(args.delay)

# extract files
# cd data/assign
# ls *.zip | xargs -n 1 unzip -n
