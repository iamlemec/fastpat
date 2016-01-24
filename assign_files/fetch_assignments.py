#!/bin/env python3

import os
import time

assign_fname = 'assign_files.txt'
assign_url_fmt = 'http://commondatastorage.googleapis.com/patents/{}/{}/{}'

url_list = []
for line in open(assign_fname):
    line = line.strip()
    if os.path.isfile(line):
        continue

    year = int(line[2:6])
    if year < 2013:
        loc = 'retro'
    else:
        loc = 'assignments'

    url = assign_url_fmt.format(loc,year,line)
    url_list.append(url)

for url in url_list:
    os.system('wget ' + url)
    time.sleep(10)

