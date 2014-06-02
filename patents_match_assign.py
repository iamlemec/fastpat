import sqlite3
from standardize import fn_match

# reset tables flag
reset = True

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

# close patent side and attach
conn_pats.close()
cur_within.execute('attach ? as patdb',(db_fname_pats,))

# merge into existing data
cur_within.execute('drop table if exists assign_info')
cur_within.execute('create table assign_info (assign_id int primary key, patnum int, source_fn int, dest_fn int, execyear int, recyear int, grantyear int, fileyear int, classone int, classtwo int)')
cur_within.execute("""insert into assign_info select assignment_use.rowid,assignment_use.patnum,source_fn,dest_fn,strftime(\'%Y\',execdate),strftime(\'%Y\',recdate),strftime(\'%Y\',grantdate),strftime(\'%Y\',filedate),classone,classtwo
                      from assignment_use left outer join assign_match on (assignment_use.rowid = assign_match.assign_id)""")

cur_within.execute('drop table if exists assign_bulk')
cur_within.execute('create table assign_bulk (source_fn int, dest_fn int, execyear int, ntrans int)')
cur_within.execute('insert into assign_bulk select source_fn,dest_fn,execyear,count(*) from assign_info group by source_fn,dest_fn,execyear')

# clean up
conn_within.commit()
conn_within.close()
