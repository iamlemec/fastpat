import os
import glob
import argparse
from itertools import chain

tables = {
    'grant': ['grant', 'ipc', 'cite'],
    'apply': ['apply', 'ipc'],
    'assign': ['assign'],
    'maint': ['maint'],
    'tmapply': ['tmapply'],
    'compustat': ['compustat'],
}

def concat_files(input, output, case, table, dryrun=False):
    system = print if dryrun else os.system

    fout = f'{output}/{case}_{table}.csv'
    files = sorted(glob.glob(f'{input}/{table}_*.csv'))

    if len(files) == 0:
        print(f'Table "{case}/{table}" not found')
        return

    first = files[0]
    flist = ' '.join(files)

    system(f'head -n 1 {first} > {fout}')
    system(f'tail -q -n +2 {flist} >> {fout}')

def concat_tables(input, output, ftype, dryrun=False):
    if not dryrun and not os.path.exists(output):
        print(f'Creating directory {output}')
        os.makedirs(output)

    for tab in tables[ftype]:
        print(f'Concat: {ftype}/{tab}')
        concat_files(input, output, ftype, tab, dryrun=dryrun)
