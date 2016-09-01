#!/bin/env python3

import os
import time

grant_dir = 'grant_files'
grant_fpath = 'meta_data/grant_files.txt'
grant_url_fmt = 'https://bulkdata.uspto.gov/data2/patent/grant/redbook/bibliographic/{}/{}'

url_list = []
for line in open(grant_fpath):
    line = line.strip()
    if os.path.isfile(line):
        continue

    if line.startswith('ipgb'):
        year = line[4:8]
    elif line.startswith('pgb'):
        year = line[3:7]
    else:
        year = line[0:4]

    fname = line
    url = grant_url_fmt.format(year,line)
    url_list.append((fname,url))

for (fname,url) in sorted(url_list):
    fpath = os.path.join(raw_dir,fname)
    print('Fetching {}'.format(fname))
    os.system('curl -o {} {}'.format(fpath,url))
    print()
    time.sleep(10)
