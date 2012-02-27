#!/usr/bin/python

import os
import time

assign_fname = 'assign_files.txt'
assign_url_fmt = 'http://commondatastorage.googleapis.com/patents/retro/2011/{}'

url_list = []
for line in open(assign_fname):
  url = assign_url_fmt.format(line.strip())
  url_list.append(url)

for url in url_list:
  os.system('wget ' + url)
  time.sleep(30)

