import sqlite3
import pandas as pd
import pandas.io.sql as sqlio

# connect to compustat db
db_fname_comp = 'store/compustat.db'
con = sqlite3.connect(db_fname_comp)
cur = con.cursor()

# read frame into memory
datf = pd.read_csv('compustat_files/comprehensive2_1950.csv',error_bad_lines=False,skiprows=1,
                   names=['gvkey','datadate','year','name','assets','capx','cash','cogs','shares',
                   'deprec','income','employ','intan','debt','prefstock','revenue','sales','rnd',
                   'fcost','price','naics','sic'])
datf_mna = pd.read_csv('compustat_files/compustat_mna.csv',error_bad_lines=False,skiprows=1,
                   names=['gvkey','datadate','year','acquire','acquire_income'])

# clean up data
datf['mktval'] = datf['shares']*datf['price']
datf = datf.drop(['datadate','shares','prefstock','price'],axis=1)
datf = datf.fillna({'naics':0})
datf['naics'] = datf['naics'].map(lambda x: int('{:<6.0f}'.format(x).replace(' ','0')))
datf = datf[~((datf['naics']>=520000)&(datf['naics']<530000))] # remove financial firms

# merge in acquisition data
datf_mna = datf_mna.drop(['datadate','acquire_income'],axis=1)
datf = datf.merge(datf_mna,how='left',on=['gvkey','year'])

# write to sql
sqlio.write_frame(datf.drop('name',axis=1),'firmyear',con,if_exists='replace')
sqlio.write_frame(datf[['gvkey','name']],'firmname',con,if_exists='replace')

# clean up and generate primary key on firmyear
cur.execute("""delete from firmyear where year=''""")
cur.execute("""delete from firmyear where rowid not in (select min(rowid) from firmyear group by gvkey,year)""")
cur.execute("""delete from firmyear where name is null""")
cur.execute("""create unique index firmyear_idx on firmyear(gvkey asc, year asc)""")

# remove duplicates and generate primary key on firmname
cur.execute("""delete from firmname where rowid not in (select max(rowid) from firmname group by gvkey,name)""")
cur.execute("""create unique index firmname_idx on firmname(gvkey asc)""")

# close db
con.commit()
con.close()
