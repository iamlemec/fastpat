# pull in all wikipedia data

import re
import os
import sys
import argparse
import sqlite3
from db_tools import ChunkInserter
from parse_common import parse_mangled_gen3, clear, get_text

# parse input arguments
parser = argparse.ArgumentParser(description='Wikipedia indexer.')
parser.add_argument('fname_in', type=str, help='filename of wikipedia')
parser.add_argument('fname_db', type=str, help='filename of database')
parser.add_argument('--limit', type=int, default=None, help='number of articles to parse')
args = parser.parse_args()

# database
con = sqlite3.connect(args.fname_db)
cur = con.cursor()
cur.execute('create table patent (patnum int, filedate text, grantdate text, ipcver text, ipccode text, state text, country text, owner text)')
pat_chunker = ChunkInserter(con, table='patent')

# parse file
i = 0
d = 0
o = 0
def handle_patent(pat):
    global i, d, o

    bib = pat.find('us-bibliographic-data-grant')

    # publication
    pub = bib.find('publication-reference')
    pubdoc = pub.find('document-id')
    patnum = get_text(pubdoc.find('doc-number'))
    if not patnum.startswith('0'):
        d += 1
        return True
    pubdate = get_text(pubdoc.find('date'))

    # application
    app = bib.find('application-reference')
    appdoc = app.find('document-id')
    appdate = get_text(appdoc.find('date'))

    # assignment
    assign = bib.find('assignees/assignee/addressbook')
    if assign is None:
        o += 1
        return True
    orgname = get_text(assign.find('orgname')).upper()
    if len(orgname) == 0:
        o += 1
        return True
    state = get_text(assign.find('address/state'))
    country = get_text(assign.find('address/country'))

    # ipc classification
    ipclist = bib.find('classifications-ipcr')
    if ipclist is not None:
        ipc = ipclist.find('classification-ipcr')
        ipcver = get_text(ipc.find('ipc-version-indicator/date'))
        isection = get_text(ipc.find('section'))
        iclass = get_text(ipc.find('class'))
        isubclass = get_text(ipc.find('subclass'))
        imaingroup = get_text(ipc.find('main-group'))
        isubgroup = get_text(ipc.find('subgroup'))
        ipccode = '%s%s%s/%2s%3s' % (isection, iclass, isubclass, imaingroup, isubgroup)
    else:
        ipcver = ''
        ipccode = ''

    pat_chunker.insert(patnum, appdate, pubdate, ipcver, ipccode, state, country, orgname)

    line = pat.sourceline
    clear(pat)

    i += 1
    if i % 100 == 0:
        print('%12d: i = %8d, d = %6d, o = %6d, pn = %s, pd = %s, ad = %s, on = %30.30s, st = %2s, ct = %2s, ipcv = %s, ipc = %s' % (line, i, d, o, patnum, pubdate, appdate, orgname, state, country, ipcver, ipccode))
    if args.limit and i >= args.limit:
        return False

    return True

# do parse
parse_mangled_gen3(args.fname_in,handle_patent)
pat_chunker.commit()
print(i)
print()
