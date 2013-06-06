import numpy as np
import sqlite3
import pandas as pd
import sys
import itertools

# execution state
if len(sys.argv) == 1:
  stage = 0
else:
  stage = int(sys.argv[1])

# load in data from db
run0 = True
if stage <= 0 and run0:
    # load data
    print 'Loading data'

    # load firm data
    # firm_life starts a firm when they file for their first patent and ends 4 years after their last file
    conn = sqlite3.connect('store/within.db')
    cur = conn.cursor()
    datf_pat = pd.DataFrame(cur.execute('select patnum,fileyear,grantyear,classone,ntrans,high_tech from grant_info').fetchall(),columns=['patnum','fileyear','grantyear','classone','ntrans','high_tech'],dtype=np.int)
    conn.close()

run1 = True
if stage <= 1 and run1:
    pass

