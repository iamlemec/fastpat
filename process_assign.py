import argparse
import sqlite3
from name_standardize import name_standardize_strong
from parse_common import ChunkInserter

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO assign fixer.')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
args = parser.parse_args()

# open database
con = sqlite3.connect(args.db)
cur = con.cursor()

# create table
cur.execute('drop table if exists assign_use')
cur.execute('create table assign_use (assignid integer primary key, patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text, assignee_state text, assignee_country text)')
chunker = ChunkInserter(con, table='assign_use')

match_num = 0
rnum = 0
for row in cur.execute('select * from assign'):
    (assignee,assignor) = (row[5],row[6])

    assignor_toks = name_standardize_strong(assignor)
    assignee_toks = name_standardize_strong(assignee)

    word_match = 0
    for tok in assignor_toks:
        if tok in assignee_toks:
            word_match += 1

    word_match /= max(1.0,0.5*(len(assignor_toks)+len(assignee_toks)))
    match = word_match > 0.5

    chunker.insert(*row)

    match_num += match
    rnum += 1

    if rnum%50000 == 0:
      print(rnum)

# commit changes
con.commit()
con.close()

# display summary
print(match_num)
print(rnum)
print(rnum-match_num)
print(float(match_num)/float(rnum))
