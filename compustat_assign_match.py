import sys
import sqlite3
import collections
from standardize import name_standardize,detect_match,detect_match_wgt

# actually store data
store = True

# connect to patents db
db_fname_pats = 'store/patents.db'
conn_pats = sqlite3.connect(db_fname_pats)
cur_pats = conn_pats.cursor()

# connect to compustat db
db_fname_comp = 'store/compustat.db'
conn_comp = sqlite3.connect(db_fname_comp)
cur_comp = conn_comp.cursor()

# connect to transfers db
if store:
  db_fname_trans = 'store/transfers.db'
  conn_trans = sqlite3.connect(db_fname_trans)
  cur_trans = conn_trans.cursor()
  cur_trans.execute('drop table if exists transfer')
  cur_trans.execute('create table transfer (patnum int, execyear int, assignor_gvkey int, assignee_gvkey int)')
  cmd_trans = 'insert into transfer values (?,?,?,?)'

# counts
rec_lim = sys.maxint
nrec = 0
match = 0

if store:
  batch_size = 1000
  transfers = []

# loop over records
for (patnum,execyear,assignor,assignee) in cur_pats.execute("""select patnum,strftime('%Y',execdate),assignor,assignee from assignment_use"""):
  assignor_match = detect_match_wgt(assignor,cur_comp)
  assignee_match = detect_match_wgt(assignee,cur_comp)

  if assignor_match != None or assignee_match != None:
    match += 1

    #print '{:8} {:5}: ({:8}) {:30.30} -> ({:8}) {:30.30}'.format(patnum,execyear,assignor_match,assignor,assignee_match,assignee)

    if store:
      transfers.append((patnum,execyear,assignor_match,assignee_match))
      if len(transfers) >= batch_size:
        cur_trans.executemany(cmd_trans,transfers)
        del transfers[:]

  nrec += 1
  if nrec > rec_lim:
    break

  if nrec % 50000 == 0:
    print nrec

# clean up and commit changes
if store:
  if len(transfers) > 0:
    cur_trans.executemany(cmd_trans,transfers)

  conn_trans.commit()
  conn_trans.close()

# close dbs
conn_pats.close()
conn_comp.close()

print match
print nrec
print float(match)/float(nrec)

