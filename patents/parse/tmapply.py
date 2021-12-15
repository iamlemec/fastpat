#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import glob
from collections import defaultdict
from traceback import print_exc
from tools.parse import *
from tools.tables import ChunkWriter, DummyWriter
from lxml.etree import iterparse

def parse_tmapply(elem, fname):
    tma = defaultdict(str)
    tma['file'] = fname

    # top-level section
    tma['serial'] = get_text(elem, 'serial-number')

    # header information
    head = elem.find('case-file-header')
    if head is not None:
        tma['regdate'] = get_text(head, 'registration-date')
        tma['filedate'] = get_text(head, 'filing-date')

    # case file statements
    stats = []
    for st in elem.findall('case-file-statements/case-file-statement'):
        tc = get_text(st, 'type-code')
        if tc.startswith('gs'):
            tx = get_text(st, 'text')
            stats.append((tc, tx))

    # summarize
    tma['n_stat'] = len(stats)
    tma['gs_codes'] = '; '.join([tc for tc, _ in stats])
    tma['statement'] = '; '.join([tx for _, tx in stats])

    # classifications
    uclas = []
    iclas = []
    for cl in elem.findall('classifications/classification'):
        uc = [u.text for u in cl.findall('us-code')]
        ic = [i.text for i in cl.findall('international-code')]
        uclas += uc
        iclas += ic
    tma['us_class'] = '; '.join(uclas)
    tma['int_class'] = '; '.join(iclas)

    # owners
    owns = []
    for ow in elem.findall('case-file-owners/case-file-owner'):
        nm = get_text(ow, 'party-name')
        owns.append(nm)
    tma['owners'] = '; '.join(dict.fromkeys(owns))

    # roll it in
    return tma

def store_tmapply(tma, chunker):
    chunker.insert(*(tma.get(k, '') for k in schema_tmapply))

# table schema
schema_tmapply = {
    'serial': 'str', # trademark number
    'regdate': 'str', # registration date
    'filedate': 'str', # filing date
    'n_stat': 'int', # number of GS statements
    'gs_codes': 'str', # all GS codes in statements
    'statement': 'str', # concatenated GS statements
    'us_class': 'str', # US classifications
    'int_class': 'str', # international classifications
    'owners': 'str', # all owner names
    'file': 'str', # path to source file
}

# file level
def parse_file(fpath, output, overwrite=False, dryrun=False, display=0):
    fdir, fname = os.path.split(fpath)
    ftag, fext = os.path.splitext(fname)

    opath = os.path.join(output, ftag)
    opath_tma = f'{opath}_tmapply.csv'

    if not overwrite:
        if os.path.exists(opath_tma):
            print(f'{ftag}: Skipping')
            return

    if not dryrun:
        chunker_tma = ChunkWriter(opath_tma, schema=schema_tmapply)
    else:
        chunker_tma = DummyWriter()

    # parse it up
    try:
        print(f'{ftag}: Starting')

        i = 0
        for event, elem in iterparse(fpath, tag='case-file', events=['end'], recover=True):
            i += 1

            # parse and store
            tma = parse_tmapply(elem, fname)
            store_tmapply(tma, chunker_tma)
            clear(elem)

            # output
            if display > 0 and i % display == 0:
                stma = {k: tma.get(k, '') for k in schema_tmapply}
                print('fn = {file:30.30s}, sn = {serial:10.10s}, rd = {regdate:10.10s}, ic = {int_class:10.10s}, gs = {gs_codes:15.15s}, st = {statement:50.50s}, ow = {owners:30.30s}'.format(**stma))

        # commit to db and close
        chunker_tma.commit()

        print(f'{ftag}: Parsed {i} trademarks')
    except Exception as e:
        print(f'{ftag}: EXCEPTION OCCURRED!')
        print_exc()

        chunker_tma.delete()

if __name__ == '__main__':
    import argparse
    from multiprocessing import Pool

    # parse input arguments
    parser = argparse.ArgumentParser(description='trademark application parser')
    parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
    parser.add_argument('--output', type=str, default='parsed/tmapply', help='directory to output to')
    parser.add_argument('--display', type=int, default=10000, help='how often to display summary')
    parser.add_argument('--dryrun', action='store_true', help='do not actually store')
    parser.add_argument('--overwrite', action='store_true', help='clobber existing files')
    parser.add_argument('--threads', type=int, default=10, help='number of threads to use')
    args = parser.parse_args()

    # collect files
    if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
        targ_dir = 'data/tmapply' if len(args.target) == 0 else args.target[0]
        file_list = sorted(glob.glob(f'{targ_dir}/apc*.xml'))
    else:
        file_list = args.target

    # ensure output dir
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    # apply options
    opts = dict(overwrite=args.overwrite, dryrun=args.dryrun, display=args.display)
    def parse_file_opts(fpath):
        parse_file(fpath, args.output, **opts)

    # parse files
    with Pool(args.threads) as pool:
        pool.map(parse_file_opts, file_list, chunksize=1)
