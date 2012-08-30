import sys
import sqlite3

# execution state
if len(sys.argv) == 1:
  stage = 0
else:
  stage = int(sys.argv[1])

# open dbs
db_fname_within = 'store/within.db'
db_fname_pats = 'store/patents.db'
db_fname_comp = 'store/compustat.db'
conn = sqlite3.connect(db_fname_within)
cur = conn.cursor()
cur.execute('attach ? as patdb',(db_fname_pats,))
cur.execute('attach ? as compdb',(db_fname_comp,))

if stage <= 0:
  # merge year data
  print 'Merge with patent data'

  cur.execute('drop table if exists grant_info')
  cur.execute('create table grant_info (patnum int primary key, firm_num int, fileyear int, grantyear int, classone int, classtwo int)')
  cur.execute('insert into grant_info select patent_use.patnum,firm_num,strftime(\'%Y\',filedate),strftime(\'%Y\',grantdate),classone,classtwo from patdb.patent_use left outer join grant_match on (patent_use.patnum = grant_match.patnum)')

  cur.execute('drop table if exists assign_info')
  cur.execute('create table assign_info (assign_id int primary key, patnum int, source_fn int, dest_fn int, execyear int, recyear int)')
  cur.execute('insert into assign_info select assignment_use.rowid,assignment_use.patnum,source_fn,dest_fn,strftime(\'%Y\',execdate),strftime(\'%Y\',recdate) from patdb.assignment_use left outer join assign_match on (assignment_use.rowid = assign_match.assign_id)')

if stage <= 1:
  # aggregate by firm-year
  print 'Aggregate by firm-year'

  cur.execute('drop table if exists source_exec')
  cur.execute('create table source_exec (firm_num int, year int, pnum int)')
  cur.execute('insert into source_exec select source_fn,execyear,count(*) from assign_info group by source_fn,execyear')

  cur.execute('drop table if exists source_file')
  cur.execute('create table source_file (firm_num int, year int, pnum int)')
  cur.execute('insert into source_file select source_fn,recyear,count(*) from assign_info group by source_fn,recyear')

  cur.execute('drop table if exists dest_exec')
  cur.execute('create table dest_exec (firm_num int, year int, pnum int)')
  cur.execute('insert into dest_exec select dest_fn,execyear,count(*) from assign_info group by dest_fn,execyear')

  cur.execute('drop table if exists dest_file')
  cur.execute('create table dest_file (firm_num int, year int, pnum int)')
  cur.execute('insert into dest_file select dest_fn,recyear,count(*) from assign_info group by dest_fn,recyear')

  cur.execute('drop table if exists file_tot')
  cur.execute('create table file_tot (firm_num int, year int, pnum int)')
  cur.execute('insert into file_tot select firm_num,fileyear,count(*) from grant_info group by firm_num,fileyear')

  cur.execute('drop table if exists grant_tot')
  cur.execute('create table grant_tot (firm_num int, year int, pnum int)')
  cur.execute('insert into grant_tot select firm_num,grantyear,count(*) from grant_info group by firm_num,grantyear')

if stage <= 2:
  # get all firm-years
  print 'Find all firm years'

  cur.execute('drop table if exists compustat_unique')
  cur.execute('create table compustat_unique (firm_num int, gvkey int)')
  cur.execute('insert into compustat_unique select firm_num,min(gvkey) from compustat group by firm_num')

  cur.execute('drop table if exists compdb.firmyear_match')
  cur.execute('create table compdb.firmyear_match (firm_num int, year int, gvkey int default null)')
  cur.execute("""insert into compdb.firmyear_match select compustat.firm_num,firmyear.year,firmyear.gvkey
                        from compdb.firmyear left outer join compustat on firmyear.gvkey = compustat.gvkey""")

  cur.execute('drop table if exists firmyear')
  cur.execute('create table firmyear (firm_num int, year int)')
  cur.execute("""insert into firmyear
        select distinct firm_num,year from source_file
  union select distinct firm_num,year from dest_file
  union select distinct firm_num,year from file_tot
  union select distinct firm_num,year from compdb.firmyear_match
  """)

  cur.execute('drop table if exists firmyear_match')
  cur.execute('create table firmyear_match (firm_num int, year int, gvkey int default null)')
  cur.execute("""insert into firmyear_match select firmyear.firm_num,firmyear.year,compustat.gvkey
                        from firmyear left outer join compustat on firmyear.firm_num = compustat.firm_num""")

  cur.execute('drop table if exists compustat_match')
  cur.execute('create table compustat_match (firm_num int, year int, gvkey int, income real default null, revenue real default null, rnd real default null, naics int default null)')
  cur.execute("""insert into compustat_match select firmyear_match.firm_num,firmyear_match.year,firmyear.gvkey,sum(firmyear.income),sum(firmyear.revenue),sum(firmyear.rnd),firmyear.naics
                 from firmyear_match left outer join compdb.firmyear on (firmyear_match.gvkey = firmyear.gvkey and firmyear_match.year = firmyear.year) group by firmyear_match.firm_num,firmyear_match.year""")

if stage <= 3:
  # merge patent data together
  print 'Merge fields together'

  cur.execute('drop table if exists firmyear_info')
  cur.execute("""create table firmyear_info (firm_num int, year int, gvkey int default null,
  source_exec_pnum int default null, source_file_pnum default null,
  dest_exec_pnum int default null, dest_file_pnum default null,
  file_pnum int default null, grant_pnum int default null,
  income real default null, revenue real default null, rnd real default null, naics int default null)""")
  cur.execute("""insert into firmyear_info select firmyear.firm_num,firmyear.year,compustat_unique.gvkey,
  source_exec.pnum,source_file.pnum,dest_exec.pnum,dest_file.pnum,file_tot.pnum,grant_tot.pnum,
  compustat_match.income, compustat_match.revenue, compustat_match.rnd, compustat_match.naics from firmyear
  left outer join source_exec      on (firmyear.firm_num = source_exec.firm_num     and firmyear.year = source_exec.year)
  left outer join source_file      on (firmyear.firm_num = source_file.firm_num     and firmyear.year = source_file.year)
  left outer join dest_exec        on (firmyear.firm_num = dest_exec.firm_num       and firmyear.year = dest_exec.year)
  left outer join dest_file        on (firmyear.firm_num = dest_file.firm_num       and firmyear.year = dest_file.year)
  left outer join file_tot         on (firmyear.firm_num = file_tot.firm_num        and firmyear.year = file_tot.year)
  left outer join grant_tot        on (firmyear.firm_num = grant_tot.firm_num       and firmyear.year = grant_tot.year)
  left outer join compustat_match  on (firmyear.firm_num = compustat_match.firm_num and firmyear.year = compustat_match.year)
  left outer join compustat_unique on (firmyear.firm_num = compustat_unique.firm_num)""")
  cur.execute('update firmyear_info set source_exec_pnum=0 where source_exec_pnum is null')
  cur.execute('update firmyear_info set source_file_pnum=0 where source_file_pnum is null')
  cur.execute('update firmyear_info set dest_exec_pnum=0 where dest_exec_pnum is null')
  cur.execute('update firmyear_info set dest_file_pnum=0 where dest_file_pnum is null')
  cur.execute('update firmyear_info set file_pnum=0 where file_pnum is null')
  cur.execute('update firmyear_info set grant_pnum=0 where grant_pnum is null')
  cur.execute('delete from firmyear_info where year is null')

if stage <= 4:
  # find set of good firm statistics
  print 'Finding firm statistics'

  cur.execute('drop table if exists firm_life')
  cur.execute('create table firm_life (firm_num int primary key, year_min int, year_max int, life_span int, has_comp bool, has_revn bool, has_rnd bool, has_pats bool)')
  cur.execute('insert into firm_life select firm_num,max(1950,min(year)),min(2012,max(year)+4),0,(sum(gvkey) not null),(sum(revenue) not null),(sum(rnd) not null),(sum(file_pnum)>0) from firmyear_info where year>=1950 and (file_pnum>0 or revenue not null) group by firm_num order by firm_num')
  cur.execute('update firm_life set life_span=year_max-year_min+1')

# clean up
conn.commit()
conn.close()

