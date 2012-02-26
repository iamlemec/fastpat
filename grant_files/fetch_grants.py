import os
import time

fid = open('dls.txt')

url_list = []

for line in fid:
  (yr_str,rest) = line.split('_')

  if yr_str.startswith('ipgb'):
    year = yr_str[4:8]
  elif yr_str.startswith('pgb'):
    year = yr_str[3:7]

  url = 'http://commondatastorage.googleapis.com/patents/grantbib/' + year + '/' + line.strip()
  url_list.append(url)

for url in url_list:
  os.system('wget ' + url)
  time.sleep(30)

