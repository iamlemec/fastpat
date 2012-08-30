import sqlite3
from standardize import fn_match

# reset tables flag
reset = False

# open dbs
db_fname_pats = 'store/patents.db'
conn_pats = sqlite3.connect(db_fname_pats)
cur_pats = conn_pats.cursor()

db_fname_within = 'store/within.db'
conn_within = sqlite3.connect(db_fname_within)
cur_within = conn_within.cursor()
cmd_check = 'select * from assign_match where assign_id=?'
cmd_firm = 'insert into firm values (?,?)'
cmd_ftok = 'insert into firm_token values (?,?,?,?)'
cmd_assign = 'insert into assign_match values (?,?,?)'

# create tables
if reset == True:
  cur_within.execute('drop table if exists assign_match')
  cur_within.execute('create table assign_match (assign_id int primary key, source_fn int, dest_fn int)')

next_fn = cur_within.execute('select max(firm_num) from firm').fetchone()[0] + 1

# loop over patent transfers
rnum = 0
for (assign_id,source_name,dest_name) in cur_pats.execute('select rowid,assignor,assignee from assignment_use'):
  if cur_within.execute(cmd_check,(assign_id,)).fetchone() != None:
    continue

  (fn_source,next_fn) = fn_match(source_name,cur_within,next_fn)
  (fn_dest,next_fn) = fn_match(dest_name,cur_within,next_fn)
  cur_within.execute(cmd_assign,(assign_id,fn_source,fn_dest))

  rnum += 1
  if rnum%10000 == 0:
    print rnum

# clean up
conn_within.commit()
conn_within.close()
conn_pats.close()

