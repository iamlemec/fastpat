#!/bin/env python3

import sys
import os

run = os.system

env = {
    'raw': 'raw',
    'fix': 'fixed',
    'sed': 'gsed' if sys.platform == 'darwin' else 'sed'
}

# commands
gen1_cmd1 = "cp {raw}/{fname} {fix}/{fname}"

gen2_cmd1 = "echo '<root>' > {fix}/{fname}"
gen2_cmd2 = "cat {raw}/{fname} | {sed} '/^<?/d' | {sed} '/^<!/d' | {sed} '/^]>/d' | {sed} 's/&/&amp;/g' >> {fix}/{fname}"
gen2_cmd3 = "echo '</root>' >> {fix}/{fname}"

gen3_cmd1 = "echo '<root>' > {fix}/{fname}"
gen3_cmd2 = "cat {raw}/{fname} | {sed} '/?xml/d' | {sed} '/!DOCTYPE/d' >> {fix}/{fname}"
gen3_cmd3 = "echo '</root>' >> {fix}/{fname}"

for f in os.listdir(raw_dir):
    if f.endswith('.dat'):
        print('Demangling %s, gen %d'%(f,1))

        fc1 = gen1_cmd1.format(fname=f,**env)

        run(fc1)
    elif f.startswith('pgb') and f.endswith('.xml'):
        print('Demangling %s, gen %d'%(f,2))

        fc1 = gen2_cmd1.format(fname=f,**env)
        fc2 = gen2_cmd2.format(fname=f,**env)
        fc3 = gen2_cmd3.format(fname=f,**env)
        fc4 = gen2_cmd4.format(fname=f,**env)

        run(fc1)
        run(fc2)
        run(fc3)
        run(fc4)
    elif f.startswith('ipgb') and f.endswith('.xml'):
        print('Demangling %s, gen %d'%(f,3))

        fc1 = gen3_cmd1.format(fname=f,**env)
        fc2 = gen3_cmd2.format(fname=f,**env)
        fc3 = gen3_cmd3.format(fname=f,**env)
        fc4 = gen3_cmd4.format(fname=f,**env)

        run(fc1)
        run(fc2)
        run(fc3)
        run(fc4)
