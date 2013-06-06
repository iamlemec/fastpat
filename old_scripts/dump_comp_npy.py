import sqlite3
import numpy as np

# file names
db_fname = 'store/compustat.db'
np_comp_fname = 'store/compustat_trans.npy'

# open db
conn = sqlite3.connect(db_fname)
cur = conn.cursor()

# fetch output arrays
cmd_fetch_comp = 'select gvkey,year,ifnull(income,0),ifnull(revenue,0),ifnull(rnd,0),ifnull(naics,0),source_pnum,dest_pnum,grant_pnum from firmyear_final order by gvkey,year'
array_type = [('gvkey',np.int),('year',np.int),('income',np.float),('revenue',np.float),('rnd',np.float),('naics',np.int),('source_pnum',np.int),('dest_pnum',np.int),('grant_pnum',np.int)]
comp_vec = np.array(cur.execute(cmd_fetch_comp).fetchall(),dtype=array_type)

#for line in cur.execute(cmd_fetch_comp):
#  print line

# save to file
np.save(np_comp_fname,comp_vec)

# close db
conn.close()

