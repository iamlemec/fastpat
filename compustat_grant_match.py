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
cmd_name = 'select name from firmname where gvkey=?'

# connect to transfers db
if store:
  db_fname_grant = 'store/transfers.db'
  conn_grant = sqlite3.connect(db_fname_grant)
  cur_grant = conn_grant.cursor()
  cur_grant.execute('drop table if exists grant_match')
  cur_grant.execute('create table grant_match (patnum int, fileyear int, grantyear int, gv_key int)')
  cmd_grant = 'insert into grant_match values (?,?,?,?)'

# counts
rec_lim = sys.maxint
nrec = 0
match = 0

if store:
  batch_size = 1000
  grants = []

# loop over records
for (patnum,fileyear,grantyear,owner) in cur_pats.execute("""select patnum,strftime('%Y',filedate),strftime('%Y',grantdate),owner from patent"""):
  owner_match = detect_match_wgt(owner,cur_comp)

  #owner_match = detect_match(owner,cur_comp)
  #if owner_match != None:
  #  (match_name,) = cur_comp.execute(cmd_name,(owner_match,)).fetchall()[0]
  #else:
  #  match_name = 'NONE'
  #if owner_match_wgt != None:
  #  (match_name_wgt,) = cur_comp.execute(cmd_name,(owner_match_wgt,)).fetchall()[0]
  #else:
  #  match_name_wgt = 'NONE'
  #if owner_match != owner_match_wgt:
  #  print '{:30.30}: {:30.30} -- {:30.30}'.format(owner,match_name,match_name_wgt)

  if owner_match != None:
    match += 1

    #print '{:8} {:4} {:4}: ({:8}) {:30.30}'.format(patnum,fileyear,grantyear,owner_match,owner)

    if store:
      grants.append((patnum,fileyear,grantyear,owner_match))
      if len(grants) >= batch_size:
        cur_grant.executemany(cmd_grant,grants)
        del grants[:]

  nrec += 1
  if nrec > rec_lim:
    break

  if nrec % 50000 == 0:
    print nrec

# clean up and commit changes
if store:
  if len(grants) > 0:
    cur_grant.executemany(cmd_grant,grants)

  conn_grant.commit()
  conn_grant.close()

# close dbs
conn_pats.close()
conn_comp.close()

print nrec
print match
print float(match)/float(nrec)

