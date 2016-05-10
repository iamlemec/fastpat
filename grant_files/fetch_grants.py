#!/bin/env python3

import os
import time

grant_fname = 'grant_files.txt'
grant_url_fmt = 'https://bulkdata.uspto.gov/data2/patent/grant/redbook/fulltext/{}/{}'

url_list = []
for line in open(grant_fname):
    line = line.strip()
    if os.path.isfile(line):
        continue

    if line.startswith('ipgb'):
        year = line[4:8]
    elif line.startswith('pgb'):
        year = line[3:7]
    else:
        year = line[0:4]

    url = grant_url_fmt.format(year,line.strip())
    url_list.append(url)

for url in url_list:
    os.system('wget ' + url)
    time.sleep(10)

