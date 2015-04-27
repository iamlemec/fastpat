import sys
import os

curl = '{}'
if sys.platform == 'darwin':
  sed = 'gsed'
else:
  sed = 'sed'

cmd1 = """echo '<root>' > {}"""
cmd2 = """cat {curl} | {sed} '/^<?/d' | {sed} '/^<!/d' | {sed} '/^]>/d' | {sed} 's/&/&amp;/g' >> {curl}""".format(sed=sed,curl=curl)
cmd3 = """echo '</root>' >> {}"""
cmd4 = """mv {} {}"""

if len(sys.argv) > 1:
  flist = sys.argv[1:]
else:
  flist = os.listdir('.')

for f in flist:
  if f.startswith('pgb') and f.endswith('.xml'):
    print f

    f2 = f + '2'

    fc1 = cmd1.format(f2)
    fc2 = cmd2.format(f,f2)
    fc3 = cmd3.format(f2)
    fc4 = cmd4.format(f2,f)

    os.system(fc1)
    os.system(fc2)
    os.system(fc3)
    os.system(fc4)
