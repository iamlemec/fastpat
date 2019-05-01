#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import glob
import argparse
import sqlite3
from lxml.etree import XMLPullParser
from collections import defaultdict
from itertools import chain
from traceback import print_exc
from parse_tools import *

def parse_apply_gen2(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 2
    pat['file'] = fname

    # top-level section
    bib = elem.find('subdoc-bibliographic-information')

    # publication data
    pub = bib.find('document-id')
    if pub is not None:
        pat['pubnum'] = get_text(pub, 'doc-number')
        pat['pubdate'] = get_text(pub, 'document-date')

    # application data
    app = bib.find('domestic-filing-data')
    if app is not None:
        pat['appnum'] = get_text(app, 'application-number/doc-number')
        pat['appdate'] = get_text(app, 'filing-date')

    # title
    tech = bib.find('technical-information')
    pat['title'] = get_text(tech, 'title-of-invention')

    # ipc code
    pat['ipcs'] = []
    ipcsec = tech.find('classification-ipc')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'classification-ipc-edition').lstrip('0')
        pat['ipcs'] = [ip for ip in gen2_ipc(ipcsec)]

    # assignee name
    pat['appname'] = get_text(bib, 'assignee/organization-name')

    # first inventor address
    resid = bib.find('inventors/first-named-inventor/residence')
    if resid is not None:
        address = resid.find('residence-us')
        if address is None:
            address = resid.find('residence-non-us')
        if address is not None:
            pat['city'] = get_text(address, 'city')
            pat['state'] = get_text(address, 'state')
            pat['country'] = get_text(address, 'country-code')

    # abstract
    abst = elem.find('subdoc-abstract')
    if abst is not None:
        pat['abstract'] = raw_text(abst, sep=' ')

    # roll it in
    return store_patent(pat)

def parse_apply_gen3(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 3
    pat['file'] = fname

    # top-level section
    bib = elem.find('us-bibliographic-data-application')
    pubref = bib.find('publication-reference')
    appref = bib.find('application-reference')

    # published patent
    pubinfo = pubref.find('document-id')
    pat['pubnum'] = get_text(pubinfo, 'doc-number')
    pat['pubdate'] = get_text(pubinfo, 'date')

    # filing date
    pat['appnum'] = get_text(appref, 'document-id/doc-number')
    pat['appdate'] = get_text(appref, 'document-id/date')
    pat['appname'] = get_text(bib, 'assignees/assignee/addressbook/orgname')

    # title
    pat['title'] = get_text(bib, 'invention-title')

    # ipc code
    pat['ipcs'] = []
    ipcsec = bib.find('classification-ipc')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'edition').lstrip('0')
        pat['ipcs'] = [ip for ip in gen3a_ipc(ipcsec)]
    else:
        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            pat['ipcver'] = get_text(ipcsec, 'classification-ipcr/ipc-version-indicator/date')
            pat['ipcs'] = [ip for ip in gen3r_ipc(ipcsec)]

    # first inventor address
    address = bib.find('parties/applicants/applicant/addressbook/address')
    if address is None:
        address = bib.find('us-parties/us-applicants/us-applicant/addressbook/address')
    if address is not None:
        pat['city'] = get_text(address, 'city')
        pat['state'] = get_text(address, 'state')
        pat['country'] = get_text(address, 'country')

    # abstract
    abspar = elem.find('abstract')
    if abspar is not None:
        pat['abstract'] = raw_text(abspar, sep=' ')

    # roll it in
    return store_patent(pat)

# parse input arguments
parser = argparse.ArgumentParser(description='patent application parser')
parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--output', type=int, default=None, help='how often to output summary')
parser.add_argument('--limit', type=int, default=None, help='only parse n patents')
args = parser.parse_args()

# table schema
schema = {
    'appnum': 'text', # Patent number
    'appdate': 'text', # Application date
    'appname': 'text', # Assignee name
    'pubnum': 'text', # Publication number
    'pubdate': 'text', # Publication date
    'ipc': 'text', # Main IPC code
    'ipcver': 'text', # IPC version info
    'city': 'text', # Assignee city
    'state': 'text', # State code
    'country': 'text', # Assignee country
    'title': 'text', # Title
    'abstract': 'text', # Abstract
    'gen': 'int', # USPTO data format
    'file': 'text', # path to source file
}
tabsig = ', '.join([f'{k} {v}' for k, v in schema.items()])

# database setup
con = sqlite3.connect(args.db)
cur = con.cursor()
cur.execute(f'CREATE TABLE IF NOT EXISTS apply ({tabsig})')
cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_appnum ON apply (appnum)')
cur.execute('CREATE TABLE IF NOT EXISTS ipc_apply (appnum text, ipc text, rank int, ver text)')
pat_chunker = ChunkInserter(con, table='apply')
ipc_chunker = ChunkInserter(con, table='ipc_apply')

# chunking express
i = 0
def store_patent(pat):
    global i
    i += 1

    an, iv = pat['appnum'], pat['ipcver']

    # store ipcs
    for j, ipc in enumerate(pat['ipcs']):
        if j == 0: pat['ipc'] = ipc
        ipc_chunker.insert(an, ipc, j, iv)

    # store patent
    pat_chunker.insert(*(pat[k] for k in schema))

    # output
    if args.output is not None and i % args.output == 0:
        print('an = {appnum:10.10s}, fd = {appdate:10.10s}, ti = {title:30.30s}, on = {appname:30.30s}, ci = {city:15.15s}, st = {state:2s}, ct = {country:2s}'.format(**{k: pat.get(k, '') for k in schema}))

    # limit
    if args.limit is not None and i >= args.limit:
        print("Reached limit.")
        return False
    else:
        return True

# collect files
if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
    targ_dir = 'data/apply' if len(args.target) == 0 else args.target[0]
    file_list = sorted(glob.glob(f'{targ_dir}/pab*.xml')) + sorted(glob.glob(f'{targ_dir}/ipab*.xml'))
else:
    file_list = args.target

# parse by generation
for fpath in file_list:
    # detect generation
    fdir, fname = os.path.split(fpath)
    if fname.startswith('pab'):
        gen = 2
        main_tag = 'patent-application-publication'
        parser = lambda fp: parse_wrapper(fp, 'patent-application-publication', parse_apply_gen2)
    elif fname.startswith('ipab'):
        gen = 3
        parser = lambda fp: parse_wrapper(fp, 'us-patent-application', parse_apply_gen3)
    else:
        raise Exception('Unknown format')

    # parse it up
    print(f'Parsing {fname}, gen {gen}')
    i0 = i
    try:
        parser(fpath)
    except Exception as e:
        print('EXCEPTION OCCURRED!')
        print_exc()
    print(f'Found {i-i0} patents, {i} total')
    print()

# commit to db and close
pat_chunker.commit()
ipc_chunker.commit()
con.close()

print(f'Found {i} patents')
