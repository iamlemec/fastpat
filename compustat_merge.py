import sqlite3

# file names
db_comp_fname = 'store/compustat.db'
db_trans_fname = 'store/transfers.db'
#db_nber_fname = 'store/nber.db'

# open compustat db and attach transfer db
conn = sqlite3.connect(db_comp_fname)
cur = conn.cursor()
cur.execute('attach ? as transdb',(db_trans_fname,))
#cur.execute('attach ? as nberdb',(db_nber_fname,))

# drop tables if they're around
cur.execute('drop table if exists source')
cur.execute('drop table if exists dest')
cur.execute('drop table if exists grant_tot')
cur.execute('drop table if exists firmyear_final')

# fetch patent transfer output arrays
cur.execute('create table source (gvkey int, year int, pnum int)')
cur.execute('insert into source select assignor_gvkey,execyear,count(*) from transdb.transfer group by assignor_gvkey,execyear having assignor_gvkey not null')
cur.execute('create table dest (gvkey int, year int, pnum int)')
cur.execute('insert into dest select assignee_gvkey,execyear,count(*) from transdb.transfer group by assignee_gvkey,execyear having assignee_gvkey not null')
cur.execute('create table grant_tot (gvkey int, year int, pnum int)')
cur.execute('insert into grant_tot select gv_key,fileyear,count(*) from transdb.grant_match group by gv_key,fileyear having gv_key not null')

# merge transfers into compustat data (without pdpco for now)
cur.execute('create table firmyear_final (gvkey int, year int, income real default null, revenue real default null, rnd real default null, naics int default null, source_pnum int default null, dest_pnum int default null, grant_pnum int default null)')
cur.execute('insert into firmyear_final select firmyear.gvkey,firmyear.year,firmyear.income,firmyear.revenue,firmyear.rnd,firmyear.naics,source.pnum,dest.pnum,grant_tot.pnum from firmyear left outer join source on (firmyear.gvkey = source.gvkey and firmyear.year = source.year) left outer join dest on (firmyear.gvkey = dest.gvkey and firmyear.year = dest.year) left outer join grant_tot on (firmyear.gvkey = grant_tot.gvkey and firmyear.year = grant_tot.year)')
cur.execute('update firmyear_final set naics=0 where naics=\'\'')
cur.execute('update firmyear_final set source_pnum=0 where source_pnum is null')
cur.execute('update firmyear_final set dest_pnum=0 where dest_pnum is null')
cur.execute('update firmyear_final set grant_pnum=0 where grant_pnum is null')

# drop tables
cur.execute('drop table source')
cur.execute('drop table dest')
cur.execute('drop table grant_tot')

# close db
conn.commit()
conn.close()

