#!/usr/bin/python

import os

grant_dir = 'grant_files'
cmd_fmt = 'python parse_abstracts_gen{}.py grant_files/{} 1'

#gen1_files = []
#gen2_files = []
gen3_files = []

for f in os.listdir(grant_dir):
  #if f.endswith('.dat'):
  #  gen1_files.append(f)

  #if f.startswith('pgb') and f.endswith('.xml'):
  #  gen2_files.append(f)

  if f.startswith('ipgb') and f.endswith('.xml'):
    gen3_files.append(f)

#gen1_files.sort()
#gen2_files.sort()
gen3_files.sort()

#for f in gen1_files:
#  print '{}: gen 1'.format(f)
#  cmd = cmd_fmt.format(1,f)
#  os.system(cmd)

#for f in gen2_files:
#  print '{}: gen 2'.format(f)
#  cmd = cmd_fmt.format(2,f)
#  os.system(cmd)

for f in gen3_files:
  print '{}: gen 3'.format(f)
  cmd = cmd_fmt.format(3,f)
  if os.system(cmd) != 0:
    print 'Subprocess failed, bailing.'
    break
