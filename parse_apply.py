#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import sys
import argparse
import sqlite3
from lxml.etree import iterparse, tostring, XMLPullParser
from copy import copy
from collections import OrderedDict
from itertools import chain
from parse_common import clear, get_text, raw_text, ChunkInserter

def gen2_ipc(ipcsec):
    ipc0 = ipcsec.find('classification-ipc-primary')
    if ipc0 is not None:
        yield get_text(ipc0, 'ipc')
    for ipc in ipcsec.findall('classification-ipc-secondary'):
        yield get_text(ipc, 'ipc')

def parse_grants_gen2(elem):
    pat = copy(default)

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
    pat['appname'] = get_text(bib, 'assignee/organization-name')

    # title
    tech = bib.find('technical-information')
    pat['title'] = get_text(tech, 'title-of-invention')

    # ipc code
    ipcsec = tech.find('classification-ipc')
    pat['ipcver'] = get_text(ipcsec, 'classification-ipc-edition')
    if ipcsec is not None:
        ipclist = list(gen2_ipc(ipcsec))
        if len(ipclist) > 0:
            pat['ipc1'] = ipclist[0]
            pat['ipc2'] = ';'.join(ipclist)

    # applicant info
    address = bib.find('correspondence-address/address')
    if address is not None:
        pat['city'] = get_text(address, 'city')
        pat['state'] = get_text(address, 'state')
        pat['country'] = get_text(address, 'country/country-code')

    # abstract
    abst = elem.find('subdoc-abstract')
    if abst is not None:
        pat['abstract'] = raw_text(abst, sep=' ')

    # roll it in
    return store_patent(pat)

def gen3_ipcr(ipcsec):
    for ipc in ipcsec.findall('classification-ipcr'):
        yield (
            '%s%s%s%s%s' % (
                get_text(ipc, 'section'),
                get_text(ipc, 'class'),
                get_text(ipc, 'subclass'),
                get_text(ipc, 'main-group'),
                get_text(ipc, 'subgroup')
            ),
            get_text(ipc, 'ipc-version-indicator/date')
        )

def gen3_ipc(ipcsec):
    ipcver = get_text(ipcsec, 'edition')
    ipc0 = get_text(ipcsec, 'main-classification')
    yield ipc0, ipcver
    for ipc in ipcsec.findall('further-classification'):
        yield (ipc.text or ''), ipcver

def parse_grants_gen3(elem):
    pat = copy(default)

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
    pat['appname'] = get_text(bib, 'assignees/assignee/orgname')

    # title
    pat['title'] = get_text(bib, 'invention-title')

    # ipc code
    ipcsec = bib.find('classifications-ipcr')
    if ipcsec is not None:
        ipclist = list(gen3_ipcr(ipcsec))
        pat['ipc1'], pat['ipcver'] = ipclist[0]
        pat['ipc2'] = ';'.join([i for i, _ in ipclist])

    ipcsec = bib.find('classification-ipc')
    if ipcsec is not None:
        ipclist = list(gen3_ipc(ipcsec))
        pat['ipc1'], pat['ipcver'] = ipclist[0]
        pat['ipc2'] = ';'.join([i for i, _ in ipclist])

    # applicant name and address
    address = bib.find('parties/applicants/applicant/addressbook/address')
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

# parse mangled xml
def parse_wrapper(fpath, main_tag, parser):
    pp = XMLPullParser(tag=main_tag, events=['end'], recover=True)
    def parse_all():
        for (_, pat) in pp.read_events():
            if not parser(pat):
                return False
        return True

    with open(fpath, errors='ignore') as f:
        pp.feed('<root>\n')
        for line in f:
            if line.startswith('<?xml'):
                if not parse_all():
                    return False
            elif line.startswith('<!DOCTYPE') or line.startswith('<!ENTITY'):
                pass
            else:
                pp.feed(line)
        else:
            pp.feed('</root>\n')
            return parse_all()

# parse input arguments
parser = argparse.ArgumentParser(description='patent application parser')
parser.add_argument('target', type=str, nargs='*', help='path of file to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--output', type=str, default=100000, help='how often to output summary')
args = parser.parse_args()

# database setup
con = sqlite3.connect(args.db)
cur = con.cursor()
cur.execute('create table if not exists apply (%s)' % sig)
cur.execute('create unique index if not exists idx_appnum on apply (appnum)')
chunker = ChunkInserter(con, table='apply')

# fields
fields = [
    'appnum', # Patent number
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

# chunking express
i = 0
def store_patent(pat):
    global i
    i += 1

    # store patent
    chunker.insert(*(pat.get(k, None) for k in fields))

    # output
    if args.output is not None and i % args.output == 0:
        print('pn = %(patnum)s, fd = %(filedate)s, gd = %(grantdate)s, on = %(owner)30.30s, ci = %(city)15.15s, st = %(state)2s, ct = %(country)2s, ocl = %(class)s, ipc = %(ipc)-10s, ver = %(ipcver)s' % {k: pat.get(k, '') for k in fields})

# collect files
if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
    targ_dir = 'apply_files' if len(args.target) == 0 else args.target[0]
    file_list = sorted(glob.glob('%s/pab*.xml' % targ_dir)) + sorted(glob.glob('%s/ipab*.xml' % targ_dir))
else:
    file_list = args.target

# parse by generation
for fpath in file_list:
    # detect generation
    (fdir, fname) = os.path.split(args.path)
    if fname.startswith('pab'):
        gen = 2
        main_tag = 'patent-application-publication'
        parser = parse_grants_gen2
    elif fname.startswith('ipab'):
        gen = 3
        main_tag = 'us-patent-application'
        parser = parse_grants_gen3
    else:
        raise Exception('Unknown format')

    # parse it up
    print('Parsing %s, gen %d' % (fname, gen))
    if not parse_wrapper(fpath, main_tag, parser):


# commit to db and close
chunker.commit()
con.close()

print('Found %d patents' % i)
