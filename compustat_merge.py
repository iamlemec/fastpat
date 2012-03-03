import sqlite3

# file names
db_comp_fname = 'store/compustat.db'
db_trans_fname = 'store/transfers.db'

# open compustat db and attach transfer db
conn = sqlite3.connect(db_comp_fname)
cur = conn.cursor()
cur.execute('attach ? as transdb',(db_trans_fname,))

# fetch output arrays
cur.execute('create table source (gvkey int, year int, pnum int)')
cur.execute('insert into source select assignor_gvkey,execyear,count(*) from transdb.transfer group by assignor_gvkey,execyear having assignor_gvkey not null')

cur.execute('create table dest (gvkey int, year int, pnum int)')
cur.execute('insert into dest select assignee_gvkey,execyear,count(*) from transdb.transfer group by assignee_gvkey,execyear having assignee_gvkey not null')

# merge into compustat data
cur.execute('create table firmyear2 (gvkey int, year int, income real default null, revenue real default null, rnd real default null, source_pnum int default null, dest_pnum int default null)')
cur.execute('insert into firmyear2 select firmyear.gvkey,firmyear.year,firmyear.income,firmyear.revenue,firmyear.rnd,source.pnum,dest.pnum from firmyear left outer join source on (firmyear.gvkey = source.gvkey and firmyear.year = source.year) left outer join dest on (firmyear.gvkey = dest.gvkey and firmyear.year = dest.year)')
cur.execute('drop table firmyear')
cur.execute('alter table firmyear2 rename to firmyear')
cur.execute('update firmyear set source_pnum=0 where source_pnum is null')
cur.execute('update firmyear set dest_pnum=0 where dest_pnum is null')

# drop tables
cur.execute('drop table source')
cur.execute('drop table dest')

# close db
conn.commit()
conn.close()

