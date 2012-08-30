import sqlite3
import itertools
from standardize import name_standardize

# open db
db_fname_pats = 'store/patents.db'
conn_pats = sqlite3.connect(db_fname_pats)
cur_pats = conn_pats.cursor()

db_fname_within = 'store/within.db'
conn_within = sqlite3.connect(db_fname_within)
cur_within = conn_within.cursor()

# create tables
cur_within.execute('drop table if exists tokens')
cur_within.execute('create table tokens (tok text)')
cmd_within = 'insert into tokens values (?)'

# batch mode
batch_size = 100000
rnum = 0
cur_pats.execute('select owner from patent')
while True:
  ret = cur_pats.fetchmany(batch_size)
  if len(ret) > 0:
    cur_within.executemany(cmd_within,zip(itertools.chain.from_iterable([name_standardize(owner) for (owner,) in ret])))
  else:
    break

  rnum += len(ret)
  print rnum

# construct frequency
cur_within.execute('drop table if exists frequency')
cur_within.execute('create table frequency (tok text, freq real)')
cur_within.execute('insert into frequency select tok,1.0/count(tok) from tokens group by tok')
cur_within.execute('create unique index tok_idx on frequency(tok asc)')
cur_within.execute('drop table tokens')

# close
conn_within.commit()
conn_within.close()
conn_pats.close()

