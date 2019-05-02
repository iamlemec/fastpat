#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import glob
import argparse
import sqlite3
from collections import defaultdict
from itertools import chain
from traceback import print_exc
from parse_tools import *

# parse it up
def parse_grant_gen1(fname):
    pat = None
    sec = None
    tag = None
    ipcver = None
    for nline in chain(open(fname, encoding='latin1', errors='ignore'), ['PATN']):
        # peek at next line
        ntag, nbuf = nline[:4].rstrip(), nline[5:-1].rstrip().lower()
        if tag is None:
            tag = ntag
            buf = nbuf
            continue
        if ntag == '':
            buf += nbuf
            continue

        # regular tags
        if tag == 'PATN':
            if pat is not None:
                pat['appnum'] = src + apn
                if not store_patent(pat):
                    break
            pat = defaultdict(str)
            sec = 'PATN'
            pat['gen'] = 1
            _, pat['file'] = os.path.split(fpath)
            pat['ipcs'] = []
            pat['cites'] = []
            src, apn = '', ''
        elif tag in ['INVT', 'ASSG', 'PRIR', 'CLAS', 'UREF', 'FREF', 'OREF', 'LREP', 'PCTA', 'ABST']:
            sec = tag
        elif tag in ['PAL', 'PAR', 'PAC', 'PA0', 'PA1']:
            if sec == 'ABST':
                if 'abstract' not in pat:
                    pat['abstract'] = buf
                else:
                    pat['abstract'] += '\n' + buf
        elif tag == 'WKU':
            if sec == 'PATN':
                pat['patnum'] = prune_patnum(buf)
        elif tag == 'SRC':
            if sec == 'PATN':
                src = '29' if buf == 'd' else buf.zfill(2) # design patents get series code 29
        elif tag == 'APN':
            if sec == 'PATN':
                apn = buf[:6]
        elif tag == 'ISD':
            if sec == 'PATN':
                pat['pubdate'] = buf
        elif tag == 'APD':
            if sec == 'PATN':
                pat['appdate'] = buf
        elif tag == 'ICL':
            if sec == 'CLAS':
                pat['ipcs'].append(pad_ipc(buf))
        elif tag == 'EDF':
            if sec == 'CLAS':
                pat['ipcver'] = buf
        elif tag == 'TTL':
            if sec == 'PATN':
                pat['title'] = buf
        elif tag == 'NCL':
            if sec == 'PATN':
                pat['claims'] = buf
        elif tag == 'NAM':
            if sec == 'ASSG':
                pat['owner'] = buf
        elif tag == 'CTY':
            if sec == 'ASSG':
                pat['city'] = buf
        elif tag == 'STA':
            if sec == 'ASSG':
                pat['state'] = buf
                pat['country'] = 'us'
        elif tag == 'CNT':
            if sec == 'ASSG':
                pat['country'] = buf[:2]
        elif tag == 'PNO':
            if sec == 'UREF':
                pat['cites'].append(prune_patnum(buf))

        # stage next tag and buf
        tag = ntag
        buf = nbuf

def parse_grant_gen2(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 2
    pat['file'] = fname

    # top-level section
    bib = elem.find('SDOBI')

    # published patent
    pubref = bib.find('B100')
    pat['patnum'] = prune_patnum(get_text(pubref, 'B110/DNUM/PDAT'))
    pat['pubdate'] = get_text(pubref, 'B140/DATE/PDAT')

    # filing date
    appref = bib.find('B200')
    pat['appnum'] = get_text(appref, 'B210/DNUM/PDAT')
    pat['appdate'] = get_text(appref, 'B220/DATE/PDAT')

    # reference info
    patref = bib.find('B500')
    ipcsec = patref.find('B510')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'B516/PDAT')
        pat['ipcs'] = [pad_ipc(ip) for ip in gen15_ipc(ipcsec)]
    else:
        pat['ipcs'] = []
    pat['title'] = get_text(patref, 'B540/STEXT/PDAT')
    pat['claims'] = get_text(patref, 'B570/B577/PDAT')

    # citations
    refs = patref.find('B560')
    if refs is not None:
        pat['cites'] = [prune_patnum(pn) for pn in gen2_cite(refs)]
    else:
        pat['cites'] = []

    # applicant name and address
    ownref = bib.find('B700/B730/B731/PARTY-US')
    if ownref is not None:
        pat['owner'] = get_text(ownref, 'NAM/ONM/STEXT/PDAT')
        address = ownref.find('ADR')
        if address is not None:
            pat['city'] = get_text(address, 'CITY/PDAT')
            pat['state'] = get_text(address, 'STATE/PDAT')
            pat['country'] = get_text(address, 'CTRY/PDAT')

    # abstract
    abspars = elem.findall('SDOAB/BTEXT/PARA')
    if len(abspars) > 0:
        pat['abstract'] = '\n'.join([raw_text(e) for e in abspars])

    # roll it in
    return store_patent(pat)

def parse_grant_gen3(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 3
    pat['file'] = fname

    # top-level section
    bib = elem.find('us-bibliographic-data-grant')
    pubref = bib.find('publication-reference')
    appref = bib.find('application-reference')

    # published patent
    pubinfo = pubref.find('document-id')
    pat['patnum'] = prune_patnum(get_text(pubinfo, 'doc-number'), maxlen=8)
    pat['pubdate'] = get_text(pubinfo, 'date')

    # filing date
    appinfo = appref.find('document-id')
    pat['appnum'] = get_text(appinfo, 'doc-number')
    pat['appdate'] = get_text(appinfo, 'date')

    # title
    pat['title'] = get_text(bib, 'invention-title')

    # ipc code
    pat['ipcs'] = []
    ipcsec = bib.find('classification-ipc')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'edition')
        pat['ipcs'] = [pad_ipc(ip) for ip in gen3g_ipc(ipcsec)]
    else:
        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            pat['ipcver'] = get_text(ipcsec, 'classification-ipcr/ipc-version-indicator/date')
            pat['ipcs'] = [ip for ip in gen3r_ipc(ipcsec)]

    # claims
    pat['claims'] = get_text(bib, 'number-of-claims')

    # citations
    refs = bib.find('references-cited')
    prefix = ''
    if refs is None:
        refs = bib.find('us-references-cited')
        prefix = 'us-'
    if refs is not None:
        pat['cites'] = [prune_patnum(pn, maxlen=8) for pn in gen3_cite(refs, prefix)]
    else:
        pat['cites'] = []

    # applicant name and address
    assignee = bib.find('assignees/assignee/addressbook')
    if assignee is not None:
        pat['owner'] = get_text(assignee, 'orgname')
        address = assignee.find('address')
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
parser = argparse.ArgumentParser(description='patent grant parser.')
parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--output', type=int, default=None, help='how often to output summary')
parser.add_argument('--limit', type=int, default=None, help='only parse n patents')
args = parser.parse_args()

# table schema
schema = {
    'patnum': 'text', # Patent number
    'pubdate': 'text', # Publication date
    'appnum': 'text', # Application number
    'appdate': 'text', # Publication date
    'ipc': 'text', # Main IPC code
    'ipcver': 'text', # IPC version info
    'city': 'text', # Assignee city
    'state': 'text', # State code
    'country': 'text', # Assignee country
    'owner': 'text', # Assignee name
    'claims': 'int', # Independent claim
    'title': 'text', # Title
    'abstract': 'text', # Abstract
    'gen': 'int', # USPTO data format
    'file': 'text', # path to source file
}
tabsig = ', '.join([f'{k} {v}' for k, v in schema.items()])

# database setup
if args.db is not None:
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    cur.execute(f'CREATE TABLE IF NOT EXISTS grant ({tabsig})')
    cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_patnum ON grant (patnum)')
    cur.execute('CREATE TABLE IF NOT EXISTS ipc_grant (patnum text, ipc text, rank int, ver text)')
    cur.execute('CREATE TABLE IF NOT EXISTS cite (src text, dst text)')
    pat_chunker = ChunkInserter(con, table='grant')
    ipc_chunker = ChunkInserter(con, table='ipc_grant')
    cit_chunker = ChunkInserter(con, table='cite')
else:
    pat_chunker = DummyInserter()
    ipc_chunker = DummyInserter()
    cit_chunker = DummyInserter()

# global adder
i = 0
def store_patent(pat):
    global i
    i += 1

    pn, iv = pat['patnum'], pat['ipcver']

    # store cites
    for cite in pat['cites']:
        cit_chunker.insert(pn, cite)

    # store ipcs
    for j, ipc in enumerate(pat['ipcs']):
        if j == 0: pat['ipc'] = ipc
        ipc_chunker.insert(pn, ipc, j, iv)

    # store patent
    pat_chunker.insert(*(pat.get(k, None) for k in schema))

    # output
    if args.output is not None and i % args.output == 0:
        print('pn = {patnum:10.10s}, pd = {pubdate:10.10s}, ti = {title:30.30s}, on = {owner:30.30s}, ci = {city:15.15s}, st = {state:2s}, ct = {country:2s}'.format(**{k: pat.get(k, '') for k in schema}))

    # limit
    if args.limit is not None and i >= args.limit:
        print("Reached limit.")
        return False
    else:
        return True

# collect files
if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
    targ_dir = 'data/grant' if len(args.target) == 0 else args.target[0]
    file_list = sorted(glob.glob(f'{targ_dir}/*.dat')) + sorted(glob.glob(f'{targ_dir}/pgb*.xml')) + sorted(glob.glob(f'{targ_dir}/ipgb*.xml'))
else:
    file_list = args.target

# parse by generation
for fpath in file_list:
    fdir, fname = os.path.split(fpath)
    if fname.endswith('.dat'):
        gen = 1
        parser = parse_grant_gen1
    elif fname.startswith('pgb'):
        gen = 2
        parser = lambda fp: parse_wrapper(fp, 'PATDOC', parse_grant_gen2)
    elif fname.startswith('ipgb'):
        gen = 3
        parser = lambda fp: parse_wrapper(fp, 'us-patent-grant', parse_grant_gen3)
    else:
        print(f'Unknown format: {fname}')

    print(f'Parsing {fname}, gen = {gen}')
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
cit_chunker.commit()

if args.db is not None:
    con.close()
