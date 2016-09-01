# pull in all wikipedia data

import argparse
import sqlite3
from db_tools import ChunkInserter
from parse_common import create_patent_table, get_text, ParserGen2

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

    bib = pat.find('SDOBI')

    # publication
    pub = bib.find('B100')
    patnum = get_text(pub.find('B110/DNUM/PDAT'))
    if not patnum.startswith('0'):
        d += 1
        return True
    pubdate = get_text(pub.find('B140/DATE/PDAT'))

    # application
    app = bib.find('B200')
    appdate = get_text(app.find('B220/DATE/PDAT'))

    # ipc classification
    klass = bib.find('B500/B510')
    ipcstr = get_text(klass.find('B511/PDAT'))
    ipcver = get_text(klass.find('B516/PDAT'))
    ipcclass = ipcstr[:4]
    ipcgroup = ipcstr[4:]

    # assignment
    assign = bib.find('B700/B730/B731/PARTY-US')
    if assign is None:
        o += 1
        return True
    orgname = get_text(assign.find('NAM/ONM/STEXT/PDAT')).upper()
    if len(orgname) == 0:
        o += 1
        return True
    state = get_text(assign.find('ADR/STATE/PDAT'))
    country = get_text(assign.find('ADR/CTRY/PDAT'))

    chunker.insert(patnum, appdate, pubdate, ipcver, ipcclass, ipcgroup, state, country, orgname)

    i += 1
    if i % 100 == 0:
        print('%12d: i = %8d, d = %6d, o = %6d, pn = %s, pd = %s, ad = %s, on = %30.30s, st = %2s, ct = %2s, ipcv = %s, ipcc = %s = ipcg = %s' % (pat.sourceline, i, d, o, patnum, pubdate, appdate, orgname, state, country, ipcver, ipcclass, ipcgroup))
    if args.limit and i >= args.limit:
        return False

    return True

# do parse
parser = ParserGen2()
parser.setContentHandler(handle_patent)
parser.parse(args.fname_in)
chunker.commit()
print(i,d,o)
print()
