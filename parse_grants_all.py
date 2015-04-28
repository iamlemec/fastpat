#!/usr/bin/python

import os
import sys

grant_dir = 'grant_files'
cmd_fmt = 'pypy parse_grants_gen{}.py grant_files/{} 1'

# collect files names for each gen
gen1_files = []
gen2_files = []
gen3_files = []

has_parsed = False
for f in os.listdir(grant_dir):
  if f.endswith('.dat'):
    gen1_files.append(f)

  if f.startswith('pgb') and f.endswith('.xml'):
    gen2_files.append(f)

  if f.startswith('ipgb') and f.endswith('.xml'):
    gen3_files.append(f)

  if f == 'parsed.txt':
    has_parsed = True

gen1_files.sort()
gen2_files.sort()
gen3_files.sort()

# get already parsed files
if has_parsed:
  parsed_fnames = filter(len,map(str.strip,list(open('grant_files/parsed.txt'))))
else:
  parsed_fnames = []
parsed_fid = open('grant_files/parsed.txt','a+')

# execute
failed_fnames = []
def parse_gen(fname,gen):
  print '{}: gen {}'.format(fname,gen)
  if fname in parsed_fnames:
    print 'Already parsed'
  else:
    cmd = cmd_fmt.format(gen,fname)
    ret = os.system(cmd)
    if ret == 0:
      print 'SUCCESS'
      parsed_fid.write(fname+'\n')
    else:
      print 'FAILED'
      failed_fnames.append(fname)
  print

for f in gen1_files: parse_gen(f,1)
for f in gen2_files: parse_gen(f,2)
for f in gen3_files: parse_gen(f,3)

if len(failed_fnames):
  print
  print 'FAILED FILES:'
  print '\n'.join(failed_fnames)
