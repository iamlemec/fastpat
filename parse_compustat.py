import sqlite3
from standardize import name_standardize

# connect to compustat db
db_fname_comp = 'store/compustat.db'
conn = sqlite3.connect(db_fname_comp)
cur = conn.cursor()

# create tables
cur.execute('drop table if exists firmyear')
cur.execute('drop table if exists firmname')
cur.execute('create table firmyear (gvkey int, year int, income real, revenue real, rnd real, employ real, cash real, intan real, naics int, sic int)')
cur.execute('create table firmname (gvkey int, name text)')

# sqlite insert commands
firmyear_cmd = 'insert into firmyear values (?,?,?,?,?,?,?,?,?,?)'
firmname_cmd = 'insert into firmname values (?,?)'

# open file
csv_fname = 'compustat_files/comprehensive2_1950.csv'
csv_fid = open(csv_fname,'r')

# store for batch commit
batch_size = 1
firm_years = []
firm_names = []
firm_toks = []

# counters
cnum = 0

# parse compustat csv
firstLine = True
for line in csv_fid:
  if firstLine:
    firstLine = False
    continue

  (gvkey,_,year,name,_,_,cash,_,_,_,income,employ,intan,_,_,revenue,_,rnd,_,_,naics,sic) = line.strip().split(',')

  # exclude finance and insurance
  if naics.startswith('52'):
    continue

  firm_years.append((gvkey,year,income,revenue,rnd,employ,cash,intan,naics,sic))
  firm_names.append((gvkey,name))

  if len(firm_years) >= batch_size:
    cur.executemany(firmyear_cmd,firm_years)
    del firm_years[:]
  if len(firm_names) >= batch_size:
    cur.executemany(firmname_cmd,firm_names)
    del firm_names[:]

  cnum += 1

# flush queue
if len(firm_years) > 0:
  cur.executemany(firmyear_cmd,firm_years)
if len(firm_names) > 0:
  cur.executemany(firmname_cmd,firm_names)

# clean up and generate primary key on firmyear
cur.execute("""delete from firmyear where year=''""")
cur.execute("""delete from firmyear where rowid not in (select min(rowid) from firmyear group by gvkey,year)""")
cur.execute("""create unique index firmyear_idx on firmyear(gvkey asc, year asc)""")

# remove duplicates and generate primary key on firmname
cur.execute("""delete from firmname where rowid not in (select max(rowid) from firmname group by gvkey,name)""")
cur.execute("""create unique index firmname_idx on firmname(gvkey asc)""")

# close db
conn.commit()
conn.close()

print cnum

