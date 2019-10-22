import os
import glob
import argparse
from itertools import chain

tables = {
    'grant': ['grant', 'ipc', 'cite'],
    'apply': ['apply', 'ipc'],
    'assign': ['assign']
}

def concat_files(input, output, case, table):
    fout = f'{output}/{case}_{table}.csv'
    files = sorted(glob.glob(f'{input}/{case}/*_{table}.csv'))

    first = files[0]
    flist = ' '.join(files)

    os.system(f'head -n 1 {first} > {fout}')
    os.system(f'tail -q -n +2 {flist} >> {fout}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='patent grant parser.')
    parser.add_argument('--input', type=str, default='parsed', help='input base directory')
    parser.add_argument('--output', type=str, default='tables', help='output directory')
    parser.add_argument('--case', type=str, default=None, help='class of data to use')
    parser.add_argument('--table', type=str, default=None, help='table to address')
    args = parser.parse_args()

    if args.case is None:
        tasks = chain(*[[(c, t) for t in ts] for c, ts in tables.items()])
    elif args.table is None:
        tasks = [(args.case, t) for t in tables[args.case]]
    else:
        tasks = [(args.case, args.table)]

    for c, t in tasks:
        print(c, t)
        concat_files(args.input, args.output, c, t)
