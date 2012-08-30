import sys
import sqlite3
from standardize import fn_match

# actually store data
store = True

# connect to compustat db
db_fname_comp = 'store/compustat.db'
conn_comp = sqlite3.connect(db_fname_comp)
cur_comp = conn_comp.cursor()

# connect to within db
db_fname_within = 'store/within.db'
conn_within = sqlite3.connect(db_fname_within)
cur_within = conn_within.cursor()

if store:
  cur_within.execute('drop table if exists compustat')
  cur_within.execute('create table compustat (gvkey int primary key, firm_num int)')
  cmd_within = 'insert into compustat values (?,?)'

# counts
rec_lim = sys.maxint
nrec = 0
match = 0

if store:
  batch_size = 1000
  firms = []

next_fn = cur_within.execute('select max(firm_num) from firm').fetchone()[0] + 1

# loop over records
for (gvkey,name) in cur_comp.execute('select gvkey,name from firmname'):
  (fn_out,next_fn) = fn_match(name,cur_within,next_fn)

  if fn_out:
    match += 1
    #(name2,) = cur_within.execute('select name from firm where firm_num=?',(fn_out,)).fetchone()
    #print '{:8d} - {:8d}: {:30.30s} {:30.30s}'.format(gvkey,fn_out,name,name2)

  if store:
    firms.append((gvkey,fn_out))
    if len(firms) >= batch_size:
      cur_within.executemany(cmd_within,firms)
      del firms[:]

  nrec += 1
  if nrec > rec_lim:
    break

  if nrec % 5000 == 0:
    print nrec

# clean up and commit changes
if store:
  if len(firms) > 0:
    cur_within.executemany(cmd_within,firms)

  conn_within.commit()

# close dbs
conn_within.close()
conn_comp.close()

print nrec
print match
print float(match)/float(nrec)

