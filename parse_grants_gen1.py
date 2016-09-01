#!/bin/env python3

import argparse
import sqlite3
from db_tools import ChunkInserter
from parse_common import create_patent_table, ParserGen1

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

# SAX hanlder for gen1 patent grants
class GrantHandler:
    def __init__(self):
        self.in_patent = False
        self.section = ''
        self.i = 0
        self.d = 0
        self.o = 0

    def tag(self, name, text):
        if len(text) == 0:
            self.section = name
            return True

        if name == 'PATN':
            if self.in_patent:
                cont = self.addPatent()
            else:
                cont = True
            self.in_patent = True
            self.patnum = ''
            self.appdate = ''
            self.pubdate = ''
            self.country = ''
            self.state = ''
            self.orgname = ''
            self.ipcstr = ''
            return cont
        elif name == 'WKU':
            if self.section == 'PATN':
                self.patnum = text
        elif name == 'APD':
            if self.section == 'PATN':
                self.appdate = text
        elif name == 'ISD':
            if self.section == 'PATN':
                self.pubdate = text
        elif name == 'CNT':
            if self.section == 'ASSG':
                self.country = text
        elif name == 'STA':
            if self.section == 'ASSG':
                self.state = text
        elif name == 'NAM':
            if self.section == 'ASSG':
                self.orgname = text
        elif name == 'ICL':
            if self.section == 'CLAS':
                self.ipcstr = text

        return True

    def addPatent(self):
        patnum = self.patnum[1:8]
        if not patnum.startswith('0'):
            self.d += 1
            return True

        appdate = self.appdate.strip()
        pubdate = self.pubdate.strip()

        ipcver = 'GEN1'
        ipcclass = self.ipcstr[:4]
        ipcgroup = self.ipcstr[4:]

        country = self.country[:2]
        state = self.state.strip()
        orgname = self.orgname.upper()
        if len(orgname) == 0:
            self.o += 1
            return True

        chunker.insert(patnum, appdate, pubdate, ipcver, ipcclass, ipcgroup, state, country, orgname)

        self.i += 1
        if self.i % 100 == 0:
            print('%12d: i = %8d, d = %6d, o = %6d, pn = %s, pd = %s, ad = %s, on = %30.30s, st = %2s, ct = %2s, ipcv = %s, ipcc = %s = ipcg = %s' % (pat.sourceline, self.i, d, o, patnum, pubdate, appdate, orgname, state, country, ipcver, ipcclass, ipcgroup))
        if args.limit and self.i >= args.limit:
            return False

# do parsing
parser = ParserGen1()
handler = GrantHandler()
parser.setContentHandler(handler)
parser.parse(in_fname)
chunker.commit()
print(handler.i)
