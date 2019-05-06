import re
import os
import sys
import glob
import sqlite3
import argparse
from lxml.etree import iterparse
from parse_tools import *
from traceback import print_exc
from collections import defaultdict

# parse assignment
def parse_assign_gen3(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 3
    pat['file'] = fname
    pat['assignid'] = None

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

    return store_patent(pat)

# parse file
def parse_file_gen3(fpath):
    _, fname = os.path.split(fpath)
    for event, elem in iterparse(fpath, tag='patent-assignment', events=['end'], recover=True):
        if not parse_assign_gen3(elem, fname):
            return False
        clear(elem)
    return True

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO patent assignment parser.')
parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--output', type=int, default=None, help='how often to output summary')
parser.add_argument('--limit', type=int, default=None, help='only parse n patents')
args = parser.parse_args()

# table schema
schema = {
    'assignid': 'integer primary key', # unique assignment id
    'patnum': 'text', # Patent number
    'execdate': 'text', # Execution date
    'recdate': 'text', # Reception date
    'conveyance': 'text', # Conveyance description
    'assignor': 'text', # Assignor name
    'assignee': 'text', # Assignee name
    'assignee_state': 'text', # State code
    'assignee_country': 'text', # Assignee country
    'gen': 'int', # USPTO data format
    'file': 'text', # path to source file
}
tabsig = ', '.join([f'{k} {v}' for k, v in schema.items()])

# connect to patent db
if args.db is not None:
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute(f'CREATE TABLE IF NOT EXISTS assign ({tabsig})')
    chunker = ChunkInserter(con, table='assign')
else:
    chunker = DummyInserter()

# chunking express
i, p = 0, 0
def store_patent(pat):
    global i, p
    i += 1

    # filter out individuals and non-transfers
    pat['assignor_type'] = org_type(pat['assignor'])
    pat['assignee_type'] = org_type(pat['assignee'])
    pat['convey_type'] = convey_type(pat['conveyance'])
    if pat['assignor_type'] == ORG_INDV or pat['assignee_type'] == ORG_INDV or pat['convey_type'] == CONV_OTHER:
        return True
    p += 1

    # store assign
    for pn in pat['patnums']:
        pat['patnum'] = pn
        chunker.insert(*(pat[k] for k in schema))

    # logging
    if args.output is not None and p % args.output == 0:
        pat['npat'] = len(pat['patnums'])
        print('[{npat:4d}]: {assignor:40.40s} [{assignor_type:1d}] -> {assignee:30.30s} [{assignee_type:1d}] ({recdate:8.8s}, {assignee_country:20.20s})'.format(**pat))

    # break
    if args.limit is not None and p >= args.limit:
        return False

    return True

# collect files
if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
    targ_dir = 'data/assign' if len(args.target) == 0 else args.target[0]
    file_list = sorted(glob.glob(f'{targ_dir}/*.xml'))
else:
    file_list = args.target

# do robust parsing
for fpath in file_list:
    print(f'Parsing {fpath}')
    i0, p0 = i, p

    try:
        parse_file_gen3(fpath)
    except Exception as e:
        print('EXCEPTION OCCURRED!')
        print_exc()

    print(f'Found {i-i0} records, {i} total')
    print(f'Found {p-p0} transfers, {p} total')
    print()

# clear out the rest
chunker.commit()

if args.db is not None:
    con.close()
