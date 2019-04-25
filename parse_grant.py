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
from parse_common import *

# parse it up
def parse_grant_gen1(fname):
    pat = None
    sec = None
    tag = None
    ipcver = None
    for nline in chain(open(fname, encoding='latin1', errors='ignore'), ['PATN']):
        # peek at next line
        ntag, nbuf = nline[:4].rstrip(), nline[5:-1].rstrip()
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
                pat['ipc1'] = ipclist.pop(0) if len(ipclist) > 0 else ''
                pat['ipc2'] = ';'.join(ipclist)
                pat['appnum'] = src + apn
                if not store_patent(pat):
                    break
            pat = defaultdict(str)
            sec = 'PATN'
            pat['gen'] = 1
            ipclist = []
            citlist = []
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
                src = buf.strip()
                src = '29' if src == 'D' else src.zfill(2) # design patents get series code 29
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
                ipclist.append(pad_ipc(buf.strip()))
        elif tag == 'EDF':
            if sec == 'CLAS':
                ipcver = buf
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
                pat['country'] = 'US'
        elif tag == 'CNT':
            if sec == 'ASSG':
                pat['country'] = buf[:2]
        elif tag == 'PNO':
            if sec == 'UREF':
                pat['citlist'].append(buf)

        # stage next tag and buf
        tag = ntag
        buf = nbuf

def parse_grant_gen2(elem):
    pat = defaultdict(str)
    pat['gen'] = 2

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
    ipclist = []
    ipcsec = patref.find('B510')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'B516/PDAT')
        ipclist = list(gen15_ipc(ipcsec))
    pat['ipc1'] = ipclist.pop(0) if len(ipclist) > 0 else ''
    pat['ipc2'] = ';'.join(ipclist)
    pat['title'] = get_text(patref, 'B540/STEXT/PDAT')
    pat['claims'] = get_text(patref, 'B570/B577/PDAT')

    # citations
    cites = []
    refs = patref.find('B560')
    if refs is not None:
        for cite in refs.findall('B561'):
            pcit = get_text(cite, 'PCIT/DOC/DNUM/PDAT')
            cites.append(pcit)
    pat['citlist'] = cites

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

def parse_grant_gen3(elem):
    pat = defaultdict(str)
    pat['gen'] = 3

    # top-level section
    bib = elem.find('us-bibliographic-data-grant')
    pubref = bib.find('publication-reference')
    appref = bib.find('application-reference')

    # published patent
    pubinfo = pubref.find('document-id')
    pat['patnum'] = prune_patnum(get_text(pubinfo, 'doc-number'))
    pat['pubdate'] = get_text(pubinfo, 'date')

    # filing date
    appinfo = appref.find('document-id')
    pat['appnum'] = get_text(appinfo, 'doc-number')
    pat['appdate'] = get_text(appinfo, 'date')

    # title
    pat['title'] = get_text(bib, 'invention-title')

    # ipc code
    ipclist = []
    ipcsec = bib.find('classification-ipc')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'edition')
        ipclist = list(gen3g_ipc(ipcsec))
    else:
        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            pat['ipcver'] = get_text(ipcsec, 'classification-ipcr/ipc-version-indicator/date')
            ipclist = list(gen3r_ipc(ipcsec))
    pat['ipc1'] = ipclist.pop(0) if len(ipclist) > 0 else ''
    pat['ipc2'] = ';'.join(ipclist)

    # claims
    pat['claims'] = get_text(bib, 'number-of-claims')

    # citations
    refs = bib.find('references-cited')
    prefix = ''
    if refs is None:
        refs = bib.find('us-references-cited')
        prefix = 'us-'
    cites = []
    if refs is not None:
        for cite in refs.findall(prefix + 'citation'):
            pcite = cite.find('patcit')
            if pcite is not None:
                docid = pcite.find('document-id')
                pnum = get_text(docid, 'doc-number')
                kind = get_text(docid, 'kind')
                if kind == 'A' or kind.startswith('B'):
                    cites.append(pnum)
    pat['citlist'] = cites

    # applicant name and address
    assignee = bib.find('assignees/assignee/addressbook')
    if assignee is not None:
        pat['owner'] = get_text(assignee, 'orgname').upper()
        address = assignee.find('address')
        if address is not None:
            pat['city'] = get_text(address, 'city').upper()
            pat['state'] = get_text(address, 'state')
            pat['country'] = get_text(address, 'country')

    # abstract
    abspar = elem.find('abstract')
    if abspar is not None:
        pat['abstract'] = raw_text(abspar, sep=' ').strip()

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
    'class': 'text', # US patent classification
    'ipc1': 'text', # Main IPC code
    'ipc2': 'text', # IPC codes
    'ipcver': 'text', # IPC version info
    'city': 'text', # Assignee city
    'state': 'text', # State code
    'country': 'text', # Assignee country
    'owner': 'text', # Assignee name
    'claims': 'int', # Independent claim
    'title': 'text', # Title
    'abstract': 'text', # Abstract
    'gen': 'int', # USPTO data format
}
tabsig = ', '.join([f'{k} {v}' for k, v in schema.items()])

# database setup
con = sqlite3.connect(args.db)
cur = con.cursor()
cur.execute(f'create table if not exists patent ({tabsig})')
cur.execute('create unique index if not exists idx_patnum on patent (patnum)')
cur.execute('create table if not exists ipc (patnum text, code text, version text)')
cur.execute('create unique index if not exists ipc_pair on ipc (patnum,code)')
cur.execute('create index if not exists ipc_patnum on ipc (patnum)')
cur.execute('create index if not exists ipc_code on ipc (code)')
cur.execute('create table if not exists cite (src int, dst int)')
cur.execute('create unique index if not exists cite_pair on cite (src,dst)')
pat_chunker = ChunkInserter(con, table='patent')
ipc_chunker = ChunkInserter(con, table='ipc')
cit_chunker = ChunkInserter(con, table='cite')

# global adder
i = 0
def store_patent(pat):
    global i
    i += 1

    # only utility patents with owners
    pat['patnum'] = prune_patnum(pat['patnum'])
    pn = pat['patnum']

    # store ipcs
    for (ipc, ver) in pat['ipclist']:
        ipc_chunker.insert(pn, ipc, ver)

    # store cites
    for cite in pat['citlist']:
        cit_chunker.insert(pn, cite)

    # store patent
    if len(pat['ipclist']) > 0:
        (pat['ipc'], pat['ipcver']) = pat['ipclist'][0]
    pat_chunker.insert(*(pat.get(k, None) for k in schema))

    # output
    if args.output is not None and i % args.output == 0:
        print('pn = %(patnum)s, fd = %(filedate)s, gd = %(grantdate)s, on = %(owner)30.30s, ci = %(city)15.15s, st = %(state)2s, ct = %(country)2s, ocl = %(class)s, ipc = %(ipc)-10s, ver = %(ipcver)s' % {k: pat.get(k, '') for k in schema})

    # limit
    if args.limit is not None and i >= args.limit:
        print("Reached limit.")
        return False
    else:
        return True

# collect files
if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
    targ_dir = 'data/grant_files' if len(args.target) == 0 else args.target[0]
    file_list = sorted(glob.glob(f'{targ_dir}/*.dat') + sorted(glob.glob(f'{targ_dir}/pgb*.xml') + sorted(glob.glob(f'{targ_dir}/ipgb*.xml')
else:
    file_list = args.target

# parse by generation
for fpath in file_list:
    fdir, fname = os.path.split(fpath)
    if fname.endswith('.dat'):
        gen = 1
        parser = parse_grants_gen1
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
con.close()
