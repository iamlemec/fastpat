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
cmd_check = 'select * from grant_match where patnum=?'
cmd_firm = 'insert into firm values (?,?)'
cmd_ftok = 'insert into firm_token values (?,?,?,?)'
cmd_grant = 'insert into grant_match values (?,?)'

# create tables
if reset:
  cur_within.execute('drop table if exists firm_token')
  cur_within.execute('drop table if exists firm')
  cur_within.execute('drop table if exists grant_match')
  cur_within.execute('create table firm_token (firm_num int, pos int, tok text, ntoks int)')
  cur_within.execute('create table firm (firm_num int primary key asc, name text)')
  cur_within.execute('create table grant_match (patnum int primary key, firm_num int)')
  cur_within.execute('create unique index firm_token_idx on firm_token (tok asc, pos asc, firm_num asc)')
  next_fn = 1
else:
  next_fn = cur_within.execute('select max(firm_num) from firm').fetchone()[0] + 1

# loop over patent grants
rnum = 0
for (patnum,owner) in cur_pats.execute('select patnum,owner from patent_use'):
  if cur_within.execute(cmd_check,(patnum,)).fetchone() != None:
    continue

  (fn_out,next_fn) = fn_match(owner,cur_within,next_fn)
  cur_within.execute(cmd_grant,(patnum,fn_out))

  rnum += 1
  if rnum%10000 == 0:
    print rnum

# close and attach patents
conn_pats.close()
cur_within.execute('attach ? as patdb',db_fname_pats)

# mere basic grant data
cur_within.execute('drop table if exists grant_basic')
cur_within.execute('create table grant_basic (patnum int primary key, firm_num int, fileyear int, grantyear int, classone int, classtwo int)')
cur_within.execute("""insert into grant_basic select patent_use.patnum,firm_num,strftime(\'%Y\',filedate),strftime(\'%Y\',grantdate),classone,classtwo
                      from patdb.patent_use left outer join grant_match on (patent_use.patnum = grant_match.patnum)""")

# clean up
conn_within.commit()
conn_within.close()
