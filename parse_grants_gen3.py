# pull in all wikipedia data

import argparse
import sqlite3
from db_tools import ChunkInserter
from parse_common import create_patent_table, get_text, ParserGen3

# parse input arguments
parser = argparse.ArgumentParser(description='Wikipedia indexer.')
parser.add_argument('fname_in', type=str, help='filename of wikipedia')
parser.add_argument('fname_db', type=str, help='filename of database')
parser.add_argument('--limit', type=int, default=None, help='number of articles to parse')
args = parser.parse_args()

# database
con = sqlite3.connect(args.fname_db)
create_patent_table(con)
chunker = ChunkInserter(con, table='patent')

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

    # ipc classification
    ipclist = bib.find('classifications-ipcr')
    if ipclist is not None:
        ipc = ipclist.find('classification-ipcr')
        ipcver = get_text(ipc.find('ipc-version-indicator/date'))
        isection = get_text(ipc.find('section'))
        imainclass = get_text(ipc.find('class'))
        isubclass = get_text(ipc.find('subclass'))
        imaingroup = get_text(ipc.find('main-group'))
        isubgroup = get_text(ipc.find('subgroup'))
        ipcclass = '%s%s%s' % (isection, imainclass, isubclass)
        ipcgroup = '%3s%s' % (imaingroup, isubgroup)
    else:
        ipcver = ''
        ipcclass = ''
        ipcgroup = ''

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

    chunker.insert(patnum, appdate, pubdate, ipcver, ipcclass, ipcgroup, state, country, orgname)

    i += 1
    if i % 100 == 0:
        print('%12d: i = %8d, d = %6d, o = %6d, pn = %s, pd = %s, ad = %s, on = %30.30s, st = %2s, ct = %2s, ipcv = %s, ipcc = %s = ipcg = %s' % (pat.sourceline, i, d, o, patnum, pubdate, appdate, orgname, state, country, ipcver, ipcclass, ipcgroup))
    if args.limit and i >= args.limit:
        return False

    return True

# do parse
parser = ParserGen3()
parser.setContentHandler(handle_patent)
parser.parse(args.fname_in)
chunker.commit()
print(i)
print()
