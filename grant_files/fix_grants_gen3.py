import sys
import os

cmd1 = """echo '<root>' > {}"""
cmd2 = """cat {} | sed '/?xml/d' | sed '/!DOCTYPE/d' >> {}"""
cmd3 = """echo '</root>' >> {}"""
cmd4 = """mv {} {}"""

for f in os.listdir('.'):
  if f.startswith('ipgb') and f.endswith('.xml'):
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

