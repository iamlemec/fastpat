#!/usr/bin/python

import sys
from lxml.etree import iterparse
import sqlite3

# handle arguments
if len(sys.argv) <= 1:
    print('Usage: parse_cites_gen3.py filename store_db')
    sys.exit(0)

in_fname = sys.argv[1]
if sys.argv[2] == '1':
    store_db = True
else:
    store_db = False

if store_db:
    # database file
    db_fname = 'store/patents.db'
    con = sqlite3.connect(db_fname)
    cur = con.cursor()
    try:
        cur.execute('create table citation (citer int, citee int)')
    except sqlite3.OperationalError as e:
        pass

def commitBatch():
    if store_db:
        cur.executemany('insert into citation values (?,?)',citations)
    del citations[:]

def get_text(parent,tag,default=''):
    child = parent.find(tag)
    return (child.text or default) if child is not None else default

def patent_gen(parent,cut):
    docid = parent.find('document-id')
    pnum = get_text(docid,'doc-number')
    kind = get_text(docid,'kind')
    return int(pnum[cut:]) if kind.startswith('B') else None

# citations generator
def citee_gen(cite_vec,prefix):
    for cite in cite_vec.findall(prefix+'citation'):
        try:
            pcite = cite.find('patcit')
            pnum = patent_gen(pcite,0)
            if pnum is not None:
                yield pnum
            else:
                pass
        except:
            pass

# store for batch commit
batch_size = 10000
citations = []

# parseahol
pcount = 0
ccount = 0
for (event,elem) in iterparse(in_fname,tag='us-patent-grant',remove_blank_text=True):
    # top-level section
    bib = elem.find('us-bibliographic-data-grant')
    pubref = bib.find('publication-reference')
    refs = bib.find('references-cited')
    prefix = ''
    if refs is None:
        refs = bib.find('us-references-cited')
        prefix = 'us-'
        if refs is None:
            continue

    # patent info
    citer = patent_gen(pubref,1)
    if citer is None:
        continue

    # roll it in
    cites = list(citee_gen(refs,prefix))
    citations += [(citer,citee) for citee in cites]
    if len(citations) >= batch_size:
        commitBatch()

    # stats
    pcount += 1
    ccount += len(cites)

# clear out the rest
if len(citations) > 0:
    commitBatch()

if store_db:
    # commit to db and close
    con.commit()
    cur.close()
    con.close()

print(pcount)
print(ccount)
