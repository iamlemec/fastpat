#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import glob
from collections import defaultdict
from traceback import print_exc
from lxml.etree import iterparse
from tools.parse import *
from tools.tables import ChunkWriter, DummyWriter

# parse assignment
def parse_assign_gen3(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 3
    pat['file'] = fname

    # top-level section
    record = elem.find('assignment-record')
    assignor = elem.find('patent-assignors')[0]
    assignee = elem.find('patent-assignees')[0]
    patents = elem.find('patent-properties')

    # conveyance
    pat['conveyance'] = get_text(record, 'conveyance-text')

    # names
    pat['assignor'] = get_text(assignor, 'name')
    pat['assignee'] = get_text(assignee, 'name')

    # dates
    pat['execdate'] = get_text(assignor, 'execution-date/date')
    pat['recdate'] = get_text(record, 'recorded-date/date')

    # location
    pat['assignee_country'] = get_text(assignee, 'country-name', default='united states')
    pat['assignee_state'] = get_text(assignee, 'state')

    # patent info
    pat['patnums'] = [prune_patnum(pn) for pn in gen3_assign(patents)]

    return pat

# parse file
def parse_file_gen3(fpath):
    _, fname = os.path.split(fpath)
    for event, elem in iterparse(fpath, tag='patent-assignment', events=['end'], recover=True):
        yield parse_assign_gen3(elem, fname)
        clear(elem)

# table schema
schema_assign = {
    'patnum': 'str', # Patent number
    'execdate': 'str', # Execution date
    'recdate': 'str', # Reception date
    'conveyance': 'str', # Conveyance description
    'assignor': 'str', # Assignor name
    'assignee': 'str', # Assignee name
    'assignee_state': 'str', # State code
    'assignee_country': 'str', # Assignee country
    'gen': 'int', # USPTO data format
    'file': 'str', # path to source file
}

# chunking express
def store_patent(pat, chunker_assign):
    # filter out individuals and non-transfers
    pat['assignor_type'] = org_type(pat['assignor'])
    pat['assignee_type'] = org_type(pat['assignee'])
    pat['convey_type'] = convey_type(pat['conveyance'])
    if pat['assignor_type'] == ORG_INDV or pat['assignee_type'] == ORG_INDV or pat['convey_type'] == CONV_OTHER:
        return

    # store assign
    for pn in pat['patnums']:
        pat['patnum'] = pn
        chunker_assign.insert(*(pat[k] for k in schema_assign))

# file level
def parse_file(fpath, output, overwrite=False, dryrun=False, display=0):
    fdir, fname = os.path.split(fpath)
    ftag, fext = os.path.splitext(fname)

    opath = os.path.join(output, ftag)
    opath_assign = f'{opath}_assign.csv'

    if not overwrite:
        if os.path.exists(opath_assign):
            print(f'{ftag}: Skipping')
            return

    if not dryrun:
        chunker_assign = ChunkWriter(opath_assign, schema=schema_assign)
    else:
        chunker_assign = DummyWriter()

    # parse it up
    try:
        print(f'{ftag}: Starting')

        i = 0
        for pat in parse_file_gen3(fpath):
            i += 1

            store_patent(pat, chunker_assign)

            # output
            if display > 0 and i % display == 0:
                pat['npat'] = len(pat['patnums'])
                print('[{npat:4d}]: {assignor:40.40s} [{assignor_type:1d}] -> {assignee:30.30s} [{assignee_type:1d}] ({recdate:8.8s}, {assignee_country:20.20s})'.format(**pat))

        print(f'{ftag}: Parsed {i} records')

        # clear out the rest
        chunker_assign.commit()
    except Exception as e:
        print(f'{ftag}: EXCEPTION OCCURRED!')
        print_exc()

        chunker_assign.delete()

if __name__ == '__main__':
    import argparse
    from multiprocessing import Pool

    # parse input arguments
    parser = argparse.ArgumentParser(description='patent application parser')
    parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
    parser.add_argument('--output', type=str, default='parsed/assign', help='directory to output to')
    parser.add_argument('--display', type=int, default=1000, help='how often to display summary')
    parser.add_argument('--dryrun', action='store_true', help='do not actually store')
    parser.add_argument('--overwrite', action='store_true', help='clobber existing files')
    parser.add_argument('--threads', type=int, default=10, help='number of threads to use')
    args = parser.parse_args()

    # collect files
    if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
        targ_dir = 'data/assign' if len(args.target) == 0 else args.target[0]
        file_list = sorted(glob.glob(f'{targ_dir}/*.xml'))
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
