#!/usr/bin/python

import os
import time

assign_fname = 'assign_files.txt'

url_list = []
for line in open(assign_fname):
  url = 'http://commondatastorage.googleapis.com/patents/retro/2011/{}'.format(line.strip())
  url_list.append(url)

for url in url_list:
  os.system('wget ' + url)
  time.sleep(30)

