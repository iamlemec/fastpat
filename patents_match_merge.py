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
db_fname_maint = 'store/maint.db'
db_fname_cites = 'store/citations.db'
con = sqlite3.connect(db_fname_within)
cur = con.cursor()
cur.execute('attach ? as patdb',(db_fname_pats,))
cur.execute('attach ? as compdb',(db_fname_comp,))
cur.execute('attach ? as maintdb',(db_fname_maint,))
cur.execute('attach ? as citedb',(db_fname_cites,))

if stage <= 0:
  # merge year data
  print 'Merge with patent data'

  cur.execute('drop table if exists grant_info')
  cur.execute('create table grant_info (patnum int primary key, firm_num int, fileyear int, grantyear int, classone int, classtwo int, high_tech int, first_trans int, ntrans int, n_cited int, n_self_cited int, n_citing int, life_grant int, life_file int, expryear int)')
  cur.execute("""insert into grant_info select grant_basic.patnum,firm_num,fileyear,grantyear,classone,classtwo,0,num_trans.first_trans,num_trans.ntrans,cite_stats.n_cited,cite_stats.n_self_cited,cite_stats.n_citing,maint.life_span,0,0 from grant_basic
                 left outer join (select patnum,min(strftime(\'%Y\',execdate)) as first_trans,count(*) as ntrans from assignment_use group by patnum) as num_trans on (grant_basic.patnum = num_trans.patnum)
                 left outer join maint on (grant_basic.patnum = maint.patnum)
                 left outer join cite_stats on (grant_basic.patnum = cite_stats.patnum)""")
  cur.execute('update grant_info set ntrans=0 where ntrans is null')
  cur.execute('update grant_info set n_cited=0 where n_cited is null')
  cur.execute('update grant_info set n_self_cited=0 where n_self_cited is null')
  cur.execute('update grant_info set n_citing=0 where n_citing is null')
  cur.execute('update grant_info set life_grant=4 where life_grant is null')
  cur.execute('update grant_info set life_file=life_grant+grantyear-fileyear')
  cur.execute('update grant_info set expryear=grantyear+life_grant')

  ht_classes = (340,375,379,701,370,345,353,367,381,382,386,235,361,365,700,708,710,713,714,719,318,706,342,343,455,438,711,716,341,712,705,707,715,717)
  cur.execute('update grant_info set high_tech=1 where classone in ('+','.join(map(str,ht_classes))+')')

  cur.execute('drop table if exists assign_info')
  cur.execute('create table assign_info (assign_id int primary key, patnum int, source_fn int, dest_fn int, execyear int, recyear int, grantyear int, fileyear int, classone int, classtwo int)')
  cur.execute("""insert into assign_info select assignment_use.rowid,assignment_use.patnum,source_fn,dest_fn,strftime(\'%Y\',execdate),strftime(\'%Y\',recdate),strftime(\'%Y\',grantdate),strftime(\'%Y\',filedate),classone,classtwo
                 from assignment_use left outer join assign_match on (assignment_use.rowid = assign_match.assign_id)""")

  cur.execute('drop table if exists assign_bulk')
  cur.execute('create table assign_bulk (source_fn int, dest_fn int, execyear int, ntrans int)')
  cur.execute('insert into assign_bulk select source_fn,dest_fn,execyear,count(*) from assign_info group by source_fn,dest_fn,execyear')

if stage <= 1:
  # aggregate by firm-year
  print 'Aggregate by firm-year'

  cur.execute('drop table if exists source_tot')
  cur.execute('create table source_tot (firm_num int, year int, nbulk int, pnum int)')
  cur.execute('insert into source_tot select source_fn,execyear,count(*),sum(ntrans) from assign_bulk group by source_fn,execyear')

  cur.execute('drop table if exists dest_tot')
  cur.execute('create table dest_tot (firm_num int, year int, nbulk int, pnum int)')
  cur.execute('insert into dest_tot select dest_fn,execyear,count(*),sum(ntrans) from assign_bulk group by dest_fn,execyear')

  cur.execute('drop table if exists file_tot')
  cur.execute('create table file_tot (firm_num int, year int, pnum int)')
  cur.execute('insert into file_tot select firm_num,fileyear,count(*) from grant_info group by firm_num,fileyear')

  cur.execute('drop table if exists grant_tot')
  cur.execute('create table grant_tot (firm_num int, year int, pnum int, n_cited int, n_self_cited int, n_citing int)')
  cur.execute('insert into grant_tot select firm_num,grantyear,count(*),sum(n_cited),sum(n_self_cited),sum(n_citing) from grant_info group by firm_num,grantyear')

  cur.execute('drop table if exists expire_tot')
  cur.execute('create table expire_tot (firm_num int, year int, pnum int)')
  cur.execute('insert into expire_tot select firm_num,expryear,count(*) from grant_info group by firm_num,expryear')

if stage <= 2:
  # get all firm-years
  print 'Find all firm years'

  cur.execute('drop table if exists compdb.firmyear_match')
  cur.execute('create table compdb.firmyear_match (firm_num int, year int, gvkey int default null)')
  cur.execute("""insert into compdb.firmyear_match select compustat.firm_num,firmyear.year,firmyear.gvkey
                        from compdb.firmyear left outer join compustat on firmyear.gvkey = compustat.gvkey""")

  cur.execute('drop table if exists firmyear_all')
  cur.execute('create table firmyear_all (firm_num int, year int)')
  cur.execute("""insert into firmyear_all
        select distinct firm_num,year from source_tot
  union select distinct firm_num,year from dest_tot
  union select distinct firm_num,year from file_tot
  union select distinct firm_num,year from grant_tot
  union select distinct firm_num,year from compdb.firmyear_match
  """)

  cur.execute('drop table if exists firmyear_match')
  cur.execute('create table firmyear_match (firm_num int, year int, gvkey int default null)')
  cur.execute("""insert into firmyear_match select firmyear_all.firm_num,firmyear_all.year,compustat.gvkey
  from firmyear_all left outer join compustat on firmyear_all.firm_num = compustat.firm_num""")

  cur.execute('drop table if exists compustat_match')
  cur.execute("""create table compustat_match (firm_num int, year int, gvkey int, assets real, capx real,
                 cash real, cogs real, deprec real, income real, employ real, intan real, debt real,
                 revenue real, sales real, rnd real, fcost real, mktval real, naics int, sic int)""")
  cur.execute("""insert into compustat_match select firmyear_match.firm_num,firmyear_match.year,firmyear.gvkey,
  sum(assets),sum(capx),sum(cash),sum(cogs),sum(deprec),sum(income),sum(employ),sum(intan),sum(debt),
  sum(revenue),sum(sales),sum(rnd),sum(fcost),sum(mktval),naics,sic
  from firmyear_match left outer join compdb.firmyear
  on (firmyear_match.gvkey = firmyear.gvkey and firmyear_match.year = firmyear.year)
  group by firmyear_match.firm_num,firmyear_match.year""")

if stage <= 3:
  # merge patent data together
  print 'Merge fields together'

  cur.execute('drop table if exists firmyear_info')
  cur.execute("""create table firmyear_info (firm_num int, year int, source_nbulk int, source_pnum int, dest_nbulk int, dest_pnum int,
  file_pnum int, grant_pnum int, expire_pnum int, n_cited int, n_self_cited int, n_citing int,
  assets real, capx real, cash real, cogs real, deprec real, income real, employ real, intan real,
  debt real, revenue real, sales real, rnd real, fcost real, mktval real, naics int, sic int)""")
  cur.execute("""insert into firmyear_info select firmyear_all.firm_num,firmyear_all.year,
  source_tot.nbulk,source_tot.pnum,dest_tot.nbulk,dest_tot.pnum,file_tot.pnum,grant_tot.pnum,expire_tot.pnum,n_cited,n_self_cited,n_citing,
  assets,capx,cash,cogs,deprec,income,employ,intan,debt,revenue,sales,rnd,fcost,mktval,naics,sic from firmyear_all
  left outer join source_tot       on (firmyear_all.firm_num = source_tot.firm_num      and firmyear_all.year = source_tot.year)
  left outer join dest_tot         on (firmyear_all.firm_num = dest_tot.firm_num        and firmyear_all.year = dest_tot.year)
  left outer join file_tot         on (firmyear_all.firm_num = file_tot.firm_num        and firmyear_all.year = file_tot.year)
  left outer join grant_tot        on (firmyear_all.firm_num = grant_tot.firm_num       and firmyear_all.year = grant_tot.year)
  left outer join expire_tot       on (firmyear_all.firm_num = expire_tot.firm_num      and firmyear_all.year = expire_tot.year)
  left outer join compustat_match  on (firmyear_all.firm_num = compustat_match.firm_num and firmyear_all.year = compustat_match.year)""")
  cur.execute('update firmyear_info set source_nbulk=0 where source_nbulk is null')
  cur.execute('update firmyear_info set source_pnum=0 where source_pnum is null')
  cur.execute('update firmyear_info set dest_nbulk=0 where dest_nbulk is null')
  cur.execute('update firmyear_info set dest_pnum=0 where dest_pnum is null')
  cur.execute('update firmyear_info set file_pnum=0 where file_pnum is null')
  cur.execute('update firmyear_info set grant_pnum=0 where grant_pnum is null')
  cur.execute('update firmyear_info set expire_pnum=0 where expire_pnum is null')
  cur.execute('delete from firmyear_info where year is null')

if stage <= 4:
  # find set of good firm statistics
  print 'Finding firm statistics'

  cur.execute('drop table if exists firm_life')
  cur.execute('create table firm_life (firm_num int primary key, year_min int, year_max int, life_span int)')
  cur.execute('insert into firm_life select firm_num,max(1950,min(year)),min(2012,max(year)),0 from firmyear_info where year>=1950 and (file_pnum>0 or source_pnum>0) group by firm_num order by firm_num')
  cur.execute('update firm_life set life_span=year_max-year_min+1')

  cur.execute('drop table if exists firm_hightech')
  cur.execute('create table firm_hightech (firm_num int, high_tech real)')
  cur.execute('insert into firm_hightech select firm_num,avg(high_tech) from grant_info group by firm_num')

  cur.execute('drop table if exists firm_class_count')
  cur.execute('create table firm_class_count (firm_num int, class int, count int)')
  cur.execute('insert into firm_class_count select firm_num,classone,count(*) from grant_info group by firm_num,classone')
  cur.execute('drop table if exists firm_class_mode')
  cur.execute('create table firm_class_mode (firm_num int, mode_class int, mode_count int)')
  # cur.execute('insert into firm_class_mode select * from firm_class_count group by firm_num having count=max(count)') # this works but isn't technically valid, best to avoid
  # in case of a tie, choose the highest classone number
  cur.execute('insert into firm_class_mode select firm_num,max(class),max(count) from (select * from firm_class_count where count = (select max(count) from firm_class_count i where i.firm_num = firm_class_count.firm_num)) group by firm_num')
  cur.execute('drop table firm_class_count')
  cur.execute('drop table if exists firm_class_mode_2')
  cur.execute('create table firm_class_mode_2 (firm_num int, mode_class int, mode_count int, tot_pats int, mode_frac real)')
  cur.execute("""insert into firm_class_mode_2 select firm_class_mode.firm_num,mode_class,mode_count,tot_pats,null from firm_class_mode left outer join
                 (select firm_num,count(*) as tot_pats from grant_match group by firm_num) as firm_pats
                 on (firm_class_mode.firm_num = firm_pats.firm_num)""")
  cur.execute('update firm_class_mode_2 set mode_frac=mode_count*1.0/tot_pats')
  cur.execute('drop table firm_class_mode')
  cur.execute('alter table firm_class_mode_2 rename to firm_class_mode')

  cur.execute('drop table if exists firm_life_2')
  cur.execute('create table firm_life_2 (firm_num int primary key, year_min int, year_max int, life_span int, high_tech real, tot_pats int, mode_class int, mode_frac real)')
  cur.execute("""insert into firm_life_2 select firm_life.firm_num,year_min,year_max,life_span,high_tech,tot_pats,mode_class,mode_frac from firm_life
                 left outer join firm_hightech on firm_life.firm_num = firm_hightech.firm_num
                 left outer join firm_class_mode on firm_life.firm_num = firm_class_mode.firm_num
                 where tot_pats>0""")
  cur.execute('drop table if exists firm_life')
  cur.execute('alter table firm_life_2 rename to firm_life')

  cur.execute('drop table firm_hightech')
  cur.execute('drop table firm_class_mode')

# clean up
con.commit()
con.close()
