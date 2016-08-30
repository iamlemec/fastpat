# pull in all wikipedia data

import re
import os
import sys
import argparse
import sqlite3
from lxml import etree
from itertools import chain
from db_tools import ChunkInserter

# parse input arguments
parser = argparse.ArgumentParser(description='Wikipedia indexer.')
parser.add_argument('fname_in', type=str, help='filename of wikipedia')
parser.add_argument('fname_db', type=str, help='filename of database')
parser.add_argument('--limit', type=int, default=None, help='number of articles to parse')
args = parser.parse_args()

# database
#con = sqlite3.connect(args.fname_db)
# cur = con.cursor()
# cur.execute('create table patent (patnum int, filedate text, grantdate text, classone int, cl    asstwo int, ipcver text, ipccode text, state text, country text, owner text)')
# pat_chunker = ChunkInserter(con, table='patent')

# preserve memory
def clear(elem):
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]

# parse file
i = 0
pp = etree.XMLPullParser(tag='us-patent-grant', events=['end'])
def handle_patents():
    global i

    for (_, pat) in pp.read_events():
        # pat_chunker.insert(aid, rid, date, atype, length, title)

        line = pat.sourceline

        clear(pat)

        i += 1
        if i % 100 == 0:
            print('%12d: i = %8d' % (line, i))
        if args.limit and i >= args.limit:
            return False
    return True

# demangle file
pp.feed('<root>\n')
with open(args.fname_in) as f:
    for line in f:
        if line.startswith('<?xml'):
            if not handle_patents():
                break
        elif line.startswith('<!DOCTYPE'):
            pass
        else:
            pp.feed(line)
pp.feed('</root>\n')

# close out
handle_patents()
# pat_chunker.commit()
print(i)
print()

