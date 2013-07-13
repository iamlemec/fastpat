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
    con = sqlite3.connect('store/within.db')
    cur = con.cursor()
    datf_pat = pd.DataFrame(cur.execute('select patnum,fileyear,grantyear,classone,high_tech,first_trans,ntrans,n_cited,n_citing,n_self_cited,life_grant,life_file from grant_info').fetchall(),columns=['patnum','fileyear','grantyear','classone','high_tech','first_trans','ntrans','n_cited','n_citing','n_self_cited','life_grant','life_file'],dtype=np.int)
    datf_trans = pd.DataFrame(cur.execute('select patnum,execyear,recyear,fileyear,grantyear,classone,classtwo from assign_info where execyear>=1950').fetchall(),columns=['patnum','execyear','recyear','fileyear','grantyear','classone','classtwo'],dtype=np.int)
    con.close()

    # cleanup
    datf_pat = datf_pat.dropna(subset=['fileyear','grantyear'])
    datf_pat['fileyear'] = datf_pat['fileyear'].astype(np.int)
    datf_pat['trans_lag_file'] = datf_pat['first_trans'] - datf_pat['fileyear']
    datf_pat['trans_lag_grant'] = datf_pat['first_trans'] - datf_pat['grantyear']
    datf_pat['pos_trans'] = datf_pat['ntrans'] > 0
    datf_pat['frac_self_cited'] = dt.noinf(datf_pat['n_self_cited'].astype(np.float)/datf_pat['n_cited'].astype(np.float))

run1 = True
if stage <= 1 and run1:
    pass

