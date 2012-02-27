#!/usr/bin/python

import os

assign_dir = 'assign_files'
cmd_fmt = './parse_assign_sax.py assign_files/{} 1'

gen_files = []

for f in os.listdir(assign_dir):
  if f.endswith('.xml'):
    gen_files.append(f)

gen_files.sort()

for f in gen_files:
  print f
  cmd = cmd_fmt.format(f)
  #print cmd
  os.system(cmd)

