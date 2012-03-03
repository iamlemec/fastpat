import sqlite3
import numpy as np

# file names
db_fname = 'store/compustat.db'
np_comp_fname = 'store/compustat_trans.npy'

# open db
conn = sqlite3.connect(db_fname)
cur = conn.cursor()

# fetch output arrays
cmd_fetch_comp = 'select gvkey,year,ifnull(income,0),ifnull(revenue,0),ifnull(rnd,0),source_pnum,dest_pnum from firmyear'
comp_vec = np.array(cur.execute(cmd_fetch_comp).fetchall())

# save to file
np.save(np_comp_fname,comp_vec)

# close db
conn.close()

