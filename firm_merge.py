import argparse
import itertools
import sqlite3
import numpy as np
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='Merge firm patent data.')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--stage', type=int, default=0, help='min stage to run')
args = parser.parse_args()

# open dbs
con = sqlite3.connect(args.db)
cur = con.cursor()

if args.stage <= 0:
  # merge year data
  print('Merging with patent data')

  cur.execute('drop table if exists patent_trans')
  cur.execute('create table patent_trans (patnum int, first_trans int, ntrans int)')
  cur.execute('insert into patent_trans select patnum,min(execyear),count(*) from assign_info group by patnum')
  cur.execute('create unique index patent_trans_idx on patent_trans(patnum)')

  cur.execute('drop table if exists patent_info')
  cur.execute('create table patent_info (patnum integer primary key, firm_num int, fileyear int, grantyear int, state text, country text, class int, ipc text, high_tech int, first_trans int, ntrans int, n_cited int, n_self_cited int, n_citing int, last_maint int, life_grant int, life_file int, expryear int)')
  cur.execute("""insert into patent_info select patent_basic.patnum,firm_num,fileyear,grantyear,state,country,class,ipc,0,patent_trans.first_trans,patent_trans.ntrans,cite_stats.n_cited,cite_stats.n_self_cited,cite_stats.n_citing,maint.last_maint,0,0,0 from patent_basic
                 left outer join patent_trans on (patent_basic.patnum = patent_trans.patnum)
                 left outer join maint on (patent_basic.patnum = maint.patnum)
                 left outer join cite_stats on (patent_basic.patnum = cite_stats.patnum)""")

  cur.execute('update patent_info set ntrans=0 where ntrans is null')
  cur.execute('update patent_info set n_cited=0 where n_cited is null')
  cur.execute('update patent_info set n_self_cited=0 where n_self_cited is null')
  cur.execute('update patent_info set n_citing=0 where n_citing is null')

  cur.execute('update patent_info set life_grant=8 where last_maint=4')
  cur.execute('update patent_info set life_grant=12 where last_maint=8')
  cur.execute('update patent_info set life_grant=17 where last_maint=12 and grantyear<1974')
  cur.execute('update patent_info set life_grant=20 where last_maint=12 and grantyear>=1974')
  cur.execute('update patent_info set life_grant=4 where last_maint is null and grantyear<2011')
  cur.execute('update patent_info set life_file=life_grant+grantyear-fileyear')
  cur.execute('update patent_info set expryear=grantyear+life_grant')

  ht_classes = [340,375,379,701,370,345,353,367,381,382,386,235,361,365,700,708,710,713,714,719,318,706,342,343,455,438,711,716,341,712,705,707,715,717]
  cur.execute('update patent_info set high_tech=1 where class in (%s)' % ','.join([str(cl) for cl in ht_classes]))

  con.commit()

if args.stage <= 1:
  # aggregate by firm-year
  print('Aggregating by firm-year')

  cur.execute('drop table if exists source_tot')
  cur.execute('create table source_tot (firm_num int, year int, nbulk int, pnum int)')
  cur.execute('insert into source_tot select source_fn,execyear,count(*),sum(ntrans) from assign_bulk group by source_fn,execyear')

  cur.execute('drop table if exists dest_tot')
  cur.execute('create table dest_tot (firm_num int, year int, nbulk int, pnum int)')
  cur.execute('insert into dest_tot select dest_fn,execyear,count(*),sum(ntrans) from assign_bulk group by dest_fn,execyear')

  cur.execute('drop table if exists file_tot')
  cur.execute('create table file_tot (firm_num int, year int, pnum int)')
  cur.execute('insert into file_tot select firm_num,fileyear,count(*) from patent_info group by firm_num,fileyear')

  cur.execute('drop table if exists grant_tot')
  cur.execute('create table grant_tot (firm_num int, year int, pnum int, n_cited int, n_self_cited int, n_citing int)')
  cur.execute('insert into grant_tot select firm_num,grantyear,count(*),sum(n_cited),sum(n_self_cited),sum(n_citing) from patent_info group by firm_num,grantyear')

  cur.execute('drop table if exists expire_tot')
  cur.execute('create table expire_tot (firm_num int, year int, pnum int)')
  cur.execute('insert into expire_tot select firm_num,expryear,count(*) from patent_info group by firm_num,expryear')

  cur.execute('drop table if exists compustat_tot')
  cur.execute("""create table compustat_tot (firm_num int, year int, gvkey int, assets real, capx real,
                 cash real, cogs real, deprec real, income real, employ real, intan real, debt real,
                 revenue real, sales real, rnd real, fcost real, mktval real, acquire real, naics int, sic int)""")
  cur.execute("""insert into compustat_tot select firm_num,year,gvkey,sum(assets),sum(capx),sum(cash),sum(cogs),sum(deprec),sum(income),
                 sum(employ),sum(intan),sum(debt),sum(revenue),sum(sales),sum(rnd),sum(fcost),sum(mktval),sum(acquire),naics,sic
                 from compustat_merge group by firm_num,year""")

  con.commit()

if args.stage <= 2:
  # merge patent data together
  print('Merging fields together')

  cur.execute('drop table if exists firmyear_all')
  cur.execute('create table firmyear_all (firm_num int, year int)')
  cur.execute("""insert into firmyear_all
        select firm_num,year from source_tot
  union select firm_num,year from dest_tot
  union select firm_num,year from file_tot
  union select firm_num,year from grant_tot
  union select firm_num,year from compustat_merge
  """)

  cur.execute('drop table if exists firmyear_info')
  cur.execute("""create table firmyear_info (firm_num int, year int, source_nbulk int, source_pnum int, dest_nbulk int, dest_pnum int,
  file_pnum int, grant_pnum int, expire_pnum int, n_cited int, n_self_cited int, n_citing int,
  assets real, capx real, cash real, cogs real, deprec real, income real, employ real, intan real,
  debt real, revenue real, sales real, rnd real, fcost real, mktval real, acquire real, naics int, sic int)""")
  cur.execute("""insert into firmyear_info select firmyear_all.firm_num,firmyear_all.year,
  source_tot.nbulk,source_tot.pnum,dest_tot.nbulk,dest_tot.pnum,file_tot.pnum,grant_tot.pnum,expire_tot.pnum,n_cited,n_self_cited,n_citing,
  assets,capx,cash,cogs,deprec,income,employ,intan,debt,revenue,sales,rnd,fcost,mktval,acquire,naics,sic from firmyear_all
  left outer join source_tot       on (firmyear_all.firm_num = source_tot.firm_num      and firmyear_all.year = source_tot.year)
  left outer join dest_tot         on (firmyear_all.firm_num = dest_tot.firm_num        and firmyear_all.year = dest_tot.year)
  left outer join file_tot         on (firmyear_all.firm_num = file_tot.firm_num        and firmyear_all.year = file_tot.year)
  left outer join grant_tot        on (firmyear_all.firm_num = grant_tot.firm_num       and firmyear_all.year = grant_tot.year)
  left outer join expire_tot       on (firmyear_all.firm_num = expire_tot.firm_num      and firmyear_all.year = expire_tot.year)
  left outer join compustat_tot    on (firmyear_all.firm_num = compustat_tot.firm_num   and firmyear_all.year = compustat_tot.year)""")
  cur.execute('update firmyear_info set source_nbulk=0 where source_nbulk is null')
  cur.execute('update firmyear_info set source_pnum=0 where source_pnum is null')
  cur.execute('update firmyear_info set dest_nbulk=0 where dest_nbulk is null')
  cur.execute('update firmyear_info set dest_pnum=0 where dest_pnum is null')
  cur.execute('update firmyear_info set file_pnum=0 where file_pnum is null')
  cur.execute('update firmyear_info set grant_pnum=0 where grant_pnum is null')
  cur.execute('update firmyear_info set expire_pnum=0 where expire_pnum is null')
  cur.execute('delete from firmyear_info where year is null')

  con.commit()

if args.stage <= 3:
  # find set of good firm statistics
  print('Finding firm statistics')

  cur.execute('drop table if exists firm_life')
  cur.execute('create table firm_life (firm_num integer primary key, year_min int, year_max int, life_span int)')
  cur.execute('insert into firm_life select firm_num,max(1950,min(year)),min(2016,max(year)),0 from firmyear_info where year>=1950 and (file_pnum>0 or source_pnum>0) group by firm_num order by firm_num')
  cur.execute('update firm_life set life_span=year_max-year_min+1')

  cur.execute('drop table if exists firm_hightech')
  cur.execute('create table firm_hightech (firm_num int, high_tech real)')
  cur.execute('insert into firm_hightech select firm_num,avg(high_tech) from patent_info group by firm_num')

  cur.execute('drop table if exists firm_tot_pats')
  cur.execute('create table firm_tot_pats (firm_num int, tot_pats int)')
  cur.execute('insert into firm_tot_pats select firm_num,count(*) from patent_basic group by firm_num')

  cur.execute('drop table if exists firm_class_count')
  cur.execute('create table firm_class_count (firm_num int, class text, count int)')
  cur.execute('insert into firm_class_count select firm_num,class,count(*) from patent_info group by firm_num,class')
  cur.execute('drop table if exists firm_class_mode')
  cur.execute('create table firm_class_mode (firm_num int, mode_class int, mode_class_count int)')
  cur.execute('insert into firm_class_mode select * from firm_class_count group by firm_num having count=max(count)') # this works but isn't technically valid
  cur.execute('drop table firm_class_count')

  cur.execute('drop table if exists firm_ipc_count')
  cur.execute('create table firm_ipc_count (firm_num int, ipc text, count int)')
  cur.execute('insert into firm_ipc_count select firm_num,ipc,count(*) from patent_info group by firm_num,ipc')
  cur.execute('drop table if exists firm_ipc_mode')
  cur.execute('create table firm_ipc_mode (firm_num int, mode_ipc text, mode_ipc_count int)')
  cur.execute('insert into firm_ipc_mode select * from firm_ipc_count group by firm_num having count=max(count)') # this works but isn't technically valid
  cur.execute('drop table firm_ipc_count')

  cur.execute('drop table if exists firm_life_2')
  cur.execute('create table firm_life_2 (firm_num integer primary key, year_min int, year_max int, life_span int, high_tech real, tot_pats int, mode_class int, mode_class_count int, mode_class_frac real, mode_ipc text, mode_ipc_count int, mode_ipc_frac real)')
  cur.execute("""insert into firm_life_2 select firm_life.firm_num,year_min,year_max,life_span,high_tech,tot_pats,mode_class,mode_class_count,0,mode_ipc,mode_ipc_count,0 from firm_life
                 left outer join firm_hightech on firm_life.firm_num = firm_hightech.firm_num
                 left outer join firm_tot_pats on firm_life.firm_num = firm_tot_pats.firm_num
                 left outer join firm_class_mode on firm_life.firm_num = firm_class_mode.firm_num
                 left outer join firm_ipc_mode on firm_life.firm_num = firm_ipc_mode.firm_num
                 where tot_pats>0""")
  cur.execute('update firm_life_2 set mode_class_frac=mode_class_count*1.0/tot_pats')
  cur.execute('update firm_life_2 set mode_ipc_frac=mode_ipc_count*1.0/tot_pats')
  cur.execute('drop table if exists firm_life')
  cur.execute('alter table firm_life_2 rename to firm_life')

  cur.execute('drop table firm_hightech')
  cur.execute('drop table firm_tot_pats')
  cur.execute('drop table firm_class_mode')
  cur.execute('drop table firm_ipc_mode')

  con.commit()

if args.stage <= 4:
  # construct patent stocks
  print('Constructing patent stocks')

  # load firm data
  firmyear_info = pd.read_sql('select * from firmyear_info',con)
  firm_info = pd.read_sql('select * from firm_life',con)

  # make (firm_num,year) index
  fnum_set = firm_info['firm_num']
  year_min = firm_info['year_min']
  year_max = firm_info['year_max']
  life_span = firm_info['life_span']
  all_fnums = np.array(list(itertools.chain.from_iterable([[fnum]*life for (fnum,life) in zip(fnum_set,life_span)])),dtype=np.int)
  all_years = np.array(list(itertools.chain.from_iterable([range(x,y+1) for (x,y) in zip(year_min,year_max)])),dtype=np.int)
  fy_all = pd.DataFrame(data={'firm_num':all_fnums,'year':all_years})
  datf_idx = fy_all.merge(firmyear_info,how='left',on=['firm_num','year'])
  datf_idx.fillna(value={'file_pnum':0,'grant_pnum':0,'expire_pnum':0,'dest_pnum':0,'source_pnum':0,'dest_nbulk':0,'source_nbulk':0},inplace=True)

  # merge in overall firm info
  datf_idx = datf_idx.merge(firm_info,how='left',on='firm_num')
  datf_idx['age'] = datf_idx['year'] - datf_idx['year_min']

  # aggregate stocks
  datf_idx['patnet'] = datf_idx['file_pnum'] - datf_idx['expire_pnum']
  firm_groups = datf_idx.groupby('firm_num')
  datf_idx['stock'] = firm_groups['patnet'].cumsum() - datf_idx['patnet']
  datf_idx = datf_idx[datf_idx['stock']>0]

  # write new frame to disk
  int_fields = ['firm_num','year','file_pnum','grant_pnum','expire_pnum',
                'dest_pnum','source_pnum','dest_nbulk','source_nbulk',
                'n_cited','n_self_cited','n_citing','naics','sic',
                'year_min','year_max','life_span','tot_pats','mode_class',
                'age','patnet','stock']
  dtype = {f: 'int' for f in int_fields}
  datf_idx.to_sql('firmyear_index',con,if_exists='replace',dtype=dtype)

  con.commit()

# clean up
con.close()
