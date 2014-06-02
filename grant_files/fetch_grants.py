#!/usr/bin/python

import os
import time

grant_fname = 'grant_files.txt'
grant_url_fmt = 'http://storage.googleapis.com/patents/grantbib/{}/{}'

url_list = []
for line in open(grant_fname):
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
  time.sleep(30)

