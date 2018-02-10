#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import glob
import argparse
import sqlite3
from lxml import etree
from copy import copy
from collections import OrderedDict, defaultdict
from itertools import chain
from parse_common import clear, get_text, raw_text, ChunkInserter
from traceback import print_exc

# parse it up
def parse_grants_gen1(fname_in, store_patent):
    pat = None
    sec = None
    tag = None
    ipcver = None
    for nline in chain(open(fname_in, encoding='latin1', errors='ignore'), ['PATN']):
        # peek at next line
        (ntag, nbuf) = (nline[:4].rstrip(), nline[5:-1].rstrip())
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
                pat['ipclist'] = [(ipc, ipcver) for ipc in pat['ipclist']]
                if not store_patent(pat):
                    break
            pat = defaultdict(str)
            pat['gen'] = 1
            pat['ipclist'] = []
            pat['citlist'] = []
            sec = 'PATN'
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
                pat['patnum'] = buf
        elif tag == 'ISD':
            if sec == 'PATN':
                pat['grantdate'] = buf
        elif tag == 'APD':
            if sec == 'PATN':
                pat['filedate'] = buf
        elif tag == 'OCL':
            if sec == 'CLAS':
                pat['class'] = buf
        elif tag == 'ICL':
            if sec == 'CLAS':
                pat['ipclist'].append(buf)
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
                pat['owner'] = buf.upper()
        elif tag == 'CTY':
            if sec == 'ASSG':
                pat['city'] = buf.upper()
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

def parse_grants_gen2(fname_in, store_patent):
    def handle_patent(elem):
        pat = defaultdict(str)
        pat['gen'] = 2

        # top-level section
        bib = elem.find('SDOBI')

        # published patent
        pubref = bib.find('B100')
        pat['patnum'] = get_text(pubref, 'B110/DNUM/PDAT')
        pat['grantdate'] = get_text(pubref, 'B140/DATE/PDAT')

        # filing date
        appref = bib.find('B200')
        pat['filedate'] = get_text(appref, 'B220/DATE/PDAT')

        # ipc code
        patref = bib.find('B500')
        ipcsec = patref.find('B510')
        ipcver = get_text(ipcsec, 'B516/PDAT')
        ipclist = []
        ipc1 = get_text(ipcsec, 'B511/PDAT')
        if ipc1 is not None:
            ipclist.append((ipc1, ipcver))
        for child in ipcsec.findall('B512'):
            ipc = get_text(child, 'PDAT')
            ipclist.append((ipc, ipcver))
        pat['ipclist'] = ipclist

        # us class
        pat['class'] = get_text(patref, 'B520/B521/PDAT')

        # citations
        cites = []
        refs = patref.find('B560')
        if refs is not None:
            for cite in refs.findall('B561'):
                pcit = get_text(cite, 'PCIT/DOC/DNUM/PDAT')
                cites.append(pcit)
        pat['citlist'] = cites

        # title
        pat['title'] = get_text(patref, 'B540/STEXT/PDAT')

        # claims
        pat['claims'] = get_text(patref, 'B570/B577/PDAT')

        # applicant name and address
        ownref = bib.find('B700/B730/B731/PARTY-US')
        if ownref is not None:
            pat['owner'] = get_text(ownref, 'NAM/ONM/STEXT/PDAT').upper()
            address = ownref.find('ADR')
            if address is not None:
                pat['city'] = get_text(address, 'CITY/PDAT').upper()
                pat['state'] = get_text(address, 'STATE/PDAT')
                pat['country'] = get_text(address, 'CTRY/PDAT', default='US')

        # abstract
        abspars = elem.findall('SDOAB/BTEXT/PARA')
        if len(abspars) > 0:
            pat['abstract'] = '\n'.join([raw_text(e) for e in abspars])

        # roll it in
        return store_patent(pat)

    # parse mangled xml
    pp = etree.XMLPullParser(tag='PATDOC', events=['end'], recover=True)
    def handle_all():
        for (_, pat) in pp.read_events():
            if not handle_patent(pat):
                return False
            clear(pat)
        return True

    with open(fname_in, errors='ignore') as f:
        pp.feed('<root>\n')
        for line in f:
            if line.startswith('<?xml'):
                if not handle_all():
                    break
            elif line.startswith('<!DOCTYPE') or line.startswith('<!ENTITY') or line.startswith(']>'):
                pass
            else:
                pp.feed(line)
        else:
            pp.feed('</root>\n')
            handle_all()

def parse_grants_gen3(fname_in, store_patent):
    def handle_patent(elem):
        pat = defaultdict(str)
        pat['gen'] = 3

        # top-level section
        bib = elem.find('us-bibliographic-data-grant')
        pubref = bib.find('publication-reference')
        appref = bib.find('application-reference')

        # published patent
        pubinfo = pubref.find('document-id')
        pat['patnum'] = get_text(pubinfo, 'doc-number')
        pat['grantdate'] = get_text(pubinfo, 'date')

        # filing date
        pat['filedate'] = get_text(appref, 'document-id/date')

        # title
        pat['title'] = get_text(bib, 'invention-title')

        # ipc code
        ipclist = []

        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            for ipc in ipcsec.findall('classification-ipcr'):
                ipclist.append(('%s%s%s%3s%s' % (get_text(ipc, 'section'),
                                                 get_text(ipc, 'class'),
                                                 get_text(ipc, 'subclass'),
                                                 get_text(ipc, 'main-group'),
                                                 get_text(ipc, 'subgroup')),
                                get_text(ipc, 'ipc-version-indicator/date')))

        ipcsec = bib.find('classification-ipc')
        if ipcsec is not None:
            ipcver = get_text(ipcsec, 'edition')
            ipc0 = ipcsec.find('main-classification')
            for ipc in chain([ipc0], ipcsec.findall('further-classification')):
                itxt = ipc.text
                itxt = itxt[:4] + itxt[4:7].replace('0',' ') + itxt[7:].replace('/','')
                ipclist.append((itxt, ipcver))

        pat['ipclist'] = ipclist

        # us class
        oclsec = bib.find('classification-national')
        if oclsec is not None:
            pat['class'] = get_text(oclsec, 'main-classification')

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
            pat['city'] = get_text(address, 'city').upper()
            pat['state'] = get_text(address, 'state')
            pat['country'] = get_text(address, 'country')

        # abstract
        abspar = elem.find('abstract')
        if abspar is not None:
            pat['abstract'] = raw_text(abspar, sep=' ')

        # roll it in
        return store_patent(pat)

    # parse mangled xml
    pp = etree.XMLPullParser(tag='us-patent-grant', events=['end'], recover=True)
    def handle_all():
        for (_, pat) in pp.read_events():
            if not handle_patent(pat):
                return False
            clear(pat)
        return True

    with open(fname_in, errors='ignore') as f:
        pp.feed('<root>\n')
        for line in f:
            if line.startswith('<?xml'):
                if not handle_all():
                    break
            elif line.startswith('<!DOCTYPE'):
                pass
            else:
                pp.feed(line)
        else:
            pp.feed('</root>\n')
            handle_all()

# MAIN SECTION

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO patent grant parser.')
parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--limit', type=int, default=None, help='only parse n patents')
args = parser.parse_args()

# database setup
con = sqlite3.connect(args.db)
cur = con.cursor()
cur.execute('create table if not exists patent (patnum int, filedate text, grantdate text, class text, ipc text, ipcver text, city text, state text, country text, owner text, claims int, title text, abstract text, gen int)')
cur.execute('create unique index if not exists idx_patnum on patent (patnum)')
cur.execute('create table if not exists ipc (patnum int, code text, version text)')
cur.execute('create unique index if not exists ipc_pair on ipc (patnum,code)')
cur.execute('create index if not exists ipc_patnum on ipc (patnum)')
cur.execute('create index if not exists ipc_code on ipc (code)')
cur.execute('create table if not exists cite (src int, dst int)')
cur.execute('create unique index if not exists cite_pair on cite (src,dst)')
pat_chunker = ChunkInserter(con, table='patent')
ipc_chunker = ChunkInserter(con, table='ipc')
cit_chunker = ChunkInserter(con, table='cite')

# fields
fields = [
    'patnum', # Patent number
    'filedate', # Application date
    'grantdate', # Publication date
    'class', # US patent classification
    'ipc', # IPC codes
    'ipcver', # IPC version info
    'city', # Assignee city
    'state', # State code
    'country', # Assignee country
    'owner', # Assignee name
    'claims', # Independent claim
    'title', # Title
    'abstract', # Abstract
    'gen', # USPTO data format
]

# global adder
i = 0
def store_patent(pat):
    global i

    # only utility patents with owners
    pat['patnum'] = pat['patnum'].lstrip('0')[:7]
    if not pat['patnum'].isnumeric() or len(pat['owner']) == 0:
        return True

    pn = pat['patnum']

    i += 1

    # store ipcs
    for (ipc, ver) in pat['ipclist']:
        ipc_chunker.insert(pn, ipc, ver)

    # store cites
    for cite in pat['citlist']:
        cit_chunker.insert(pn, cite)

    # store patent
    if len(pat['ipclist']) > 0:
        (pat['ipc'], pat['ipcver']) = pat['ipclist'][0]
    pat_chunker.insert(*(pat.get(k, None) for k in fields))

    # output
    if i % 1000 == 0:
        print('pn = %(patnum)s, fd = %(filedate)s, gd = %(grantdate)s, on = %(owner)30.30s, ci = %(city)15.15s, st = %(state)2s, ct = %(country)2s, ocl = %(class)s, ipc = %(ipc)-10s, ver = %(ipcver)s' % { k: pat.get(k, '') for k in fields })

    # limit
    if args.limit and i >= args.limit:
        return False
    return True

# collect files
if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
    targ_dir = 'grant_files' if len(args.target) == 0 else args.target[0]
    file_list = sorted(glob.glob('%s/*.dat' % targ_dir)) \
              + sorted(glob.glob('%s/pgb*.xml' % targ_dir)) \
              + sorted(glob.glob('%s/ipgb*.xml' % targ_dir))
else:
    file_list = args.target

# parse by generation
for fpath in file_list:

    # Terminate upon reaching limit
    if args.limit is not None and i >= args.limit:
        print("Reached limit.")
        break

    (fdir, fname) = os.path.split(fpath)
    if fname.endswith('.dat'):
        gen = 1
        parser = parse_grants_gen1
    elif fname.startswith('pgb'):
        gen = 2
        parser = parse_grants_gen2
    elif fname.startswith('ipgb'):
        gen = 3
        parser = parse_grants_gen3
    else:
        raise(Exception('Unknown format'))

    print('Parsing %s, gen = %d' % (fname, gen))
    i0 = i
    try:
        parser(fpath, store_patent)
    except Exception as e:
        print('EXCEPTION OCCURRED!')
        print_exc()
    print('Found %d patents, %d total' % (i-i0, i))
    print()

# commit to db and close
pat_chunker.commit()
ipc_chunker.commit()
cit_chunker.commit()
con.close()
