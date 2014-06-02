import sqlite3
import pandas as pd
import pandas.io.sql as sqlio

# connect to nber db
db_fname = 'store/maint.db'
con = sqlite3.connect(db_fname)
cur = con.cursor()

# import to dataframe
print 'Writing table'

datf = pd.read_csv('maint_files/MaintFeeEvents_20140310.txt',sep=' ',error_bad_lines=False,header=0,usecols=[0,2,6],names=['patnum','is_small','event_code'])
cur.execute('drop table if exists maint')
sqlio.write_frame(datf,'maint',con)

# clean up data
print 'Data cleanup'

str_list = lambda sv: '('+','.join(map(lambda s: '\''+s+'\'',sv))+')'
maint_4yr  = str_list(['M1551','M170','M173','M183','M2551','M273','M283'])
maint_8yr  = str_list(['M1552','M171','M174','M184','M2552','M274','M284'])
maint_12yr = str_list(['M1553','M172','M175','M185','M2553','M275','M285'])

cur.execute('drop table if exists maint2')
cur.execute('create table maint2 (patnum int, is_small int, lag int, event_code text)')
cur.execute('insert into maint2 select patnum,is_small==\'Y\',0,event_code from maint')
cur.execute('update maint2 set lag=4 where event_code in '+maint_4yr)
cur.execute('update maint2 set lag=8 where event_code in '+maint_8yr)
cur.execute('update maint2 set lag=12 where event_code in '+maint_12yr)
cur.execute('delete from maint2 where lag==0')
cur.execute('drop table maint')
cur.execute('create table maint (patnum int, is_small int, last_maint int, life_span int)')
cur.execute('insert into maint select patnum,max(is_small),max(lag),0 from maint2 group by patnum')
cur.execute('drop table maint2')
cur.execute('update maint set life_span=8 where last_maint==4')
cur.execute('update maint set life_span=12 where last_maint==8')
cur.execute('update maint set life_span=17 where last_maint==12')

# commit and close
con.commit()
con.close()

