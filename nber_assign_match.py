import sys
import sqlite3
import collections
from standardize import name_standardize

# connect to patents db
db_fname_pats = 'store/patents.db'
conn_pats = sqlite3.connect(db_fname_pats)
cur_pats = conn_pats.cursor()

# connect to compustat db
db_fname_nber = 'store/nber.db'
conn_nber = sqlite3.connect(db_fname_nber)
cur_nber = conn_nber.cursor()

# connect to transfers db
db_fname_trans = 'store/transfers.db'
conn_trans = sqlite3.connect(db_fname_trans)
cur_trans = conn_trans.cursor()
cur_trans.execute('drop table if exists transfer_nber')
cur_trans.execute('create table transfer_nber (patnum int, execyear int, assignor_gvkey int, assignee_gvkey int)')

# db commands
cmd_key = 'select gvkey,idx,ntoks from gv_keyword where keyword=?'
cmd_name = 'select name from gv_name where gvkey=?'
cmd_trans = 'insert into transfer_nber values (?,?,?,?)'

# detect matches
match_cut = 1.0
def detect_match(name):
  gv_match = collections.defaultdict(float)

  keys = name_standardize(name)
  nkeys = len(keys)

  for (key,idx) in zip(keys,range(nkeys)):
    for (gvkey,pos,ntoks) in cur_nber.execute(cmd_key,(key,)):
      if idx == pos:
        gv_match[gvkey] += 1.0/max(nkeys,ntoks)

  gv_out = None
  best_gv = None
  if len(gv_match) > 0:
    best_gv = max(gv_match,key=gv_match.get)
    best_val = gv_match[best_gv]
    if best_val >= match_cut:
      gv_out = best_gv

  #if gv_out == None and best_gv != None:
  #  for (name,) in cur_nber.execute(cmd_name,(best_gv,)):
  #    print '{:8}: {:40.40}: {}'.format(best_gv,name,', '.join(keys))

  return gv_out

# counts
rec_lim = sys.maxint
nrec = 0
match = 0

batch_size = 1000
transfers = []

# loop over records
for (patnum,execyear,assignor,assignee) in cur_pats.execute("""select patnum,strftime('%Y',execdate),assignor,assignee from assignment"""):
  assignor_match = detect_match(assignor)
  assignee_match = detect_match(assignee)

  if assignor_match != None or assignee_match != None:
    match += 1

    #print '{:8} {:5}: ({:8}) {:30.30} -> ({:8}) {:30.30}'.format(patnum,execyear,assignor_match,assignor,assignee_match,assignee)

    transfers.append((patnum,execyear,assignor_match,assignee_match))
    if len(transfers) >= batch_size:
      cur_trans.executemany(cmd_trans,transfers)
      del transfers[:]

  nrec += 1
  if nrec > rec_lim:
    break

  if nrec%50000 == 0:
    print nrec

# clean up
if len(transfers) > 0:
  cur_trans.executemany(cmd_trans,transfers)

# commit changes
conn_trans.commit()

# close dbs
conn_pats.close()
conn_nber.close()
conn_trans.close()

print match
print nrec
print float(match)/float(nrec)

