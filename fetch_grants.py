#!/bin/env python3

import os
import time

grant_dir = 'grant_files'
grant_fpath = 'meta_data/grant_files.txt'
grant_url_fmt = 'https://bulkdata.uspto.gov/data/patent/grant/redbook/bibliographic/{}/{}'
overwrite = False

url_list = []
for line in open(grant_fpath):
    line = line.strip()
    path = os.path.join(grant_dir, line)
    if not overwrite and os.path.isfile(path):
        continue

    if line.startswith('ipgb'):
        year = line[4:8]
    elif line.startswith('pgb'):
        year = line[3:7]
    else:
        year = line[0:4]

    url = grant_url_fmt.format(year, line)
    url_list.append((line, path, url))

for (name, path, url) in sorted(url_list):
    print('Fetching %s' % name)
    os.system('curl -o %s %s' % (path, url))
    print()
    time.sleep(10)

# to extract:
# cd grant_files
# ls -1 | xargs -n 1 unzip
# rm *.txt
# rm *.html
