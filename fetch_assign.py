#!/bin/env python3

import os
import time

assign_dir = 'assign_files'
assign_fname = 'meta_data/assign_files.txt'
assign_url_fmt = 'https://bulkdata.uspto.gov/data/patent/assignment/{}'
overwrite = False

url_list = []
for line in open(assign_fname):
    line = line.strip()
    path = os.path.join(assign_dir, line)
    if not overwrite and os.path.isfile(path):
        continue

    year = int(line[2:6])

    url = assign_url_fmt.format(line)
    url_list.append((line, path, url))

for (name, path, url) in sorted(url_list):
    print('Fetching %s' % name)
    os.system('curl -o %s %s' % (path, url))
    print()
    time.sleep(10)

# extract files
# ls -1 | xargs -n 1 unzip
