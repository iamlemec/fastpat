import sqlite3
from standardize import name_standardize

# connect to compustat db
db_fname_comp = 'store/compustat.db'
conn = sqlite3.connect(db_fname_comp)
cur = conn.cursor()

# create tables
cur.execute('drop table if exists firmyear')
cur.execute('drop table if exists firmname')
cur.execute('drop table if exists firmkey')
cur.execute('create table firmyear (gvkey int, year int, income real default null, revenue real default null, rnd real default null, naics int default null)')
cur.execute('create table firmname (gvkey int, name text)')
cur.execute('create table firmkey (gvkey int, idx int, keyword text, ntoks int)')

# sqlite insert commands
firmyear_cmd = 'insert into firmyear values (?,?,?,?,?,?)'
firmname_cmd = 'insert into firmname values (?,?)'
firmkey_cmd = 'insert into firmkey values (?,?,?,?)'

# open file
csv_fname = 'compustat_files/all_firms_1950.csv'
csv_fid = open(csv_fname,'r')

# store for batch commit
batch_size = 1000
firm_years = []
firm_names = []
firm_toks = []

# counters
cnum = 0
fnum = 0

# parse compustat csv
firstLine = True
for line in csv_fid:
  if firstLine:
    firstLine = False
    continue

  (gvkey,_,year,name,_,income,revenue,rnd,naics) = line.strip().split(',')

  # exclude finance and insurance
  if naics.startswith('52'):
    continue

  if len(income) == 0:
    income = None
  if len(revenue) == 0:
    revenue = None
  if len(rnd) == 0:
    rnd = None

  firm_years.append((gvkey,year,income,revenue,rnd,naics))
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

# cursor for insertion
cur_key = conn.cursor()

# store tokens
for row in cur.execute('select * from firmname'):
  (gvkey,name) = row

  name_toks = name_standardize(name)

  #print '{:40.40}: {}'.format(name,', '.join(name_toks))

  ntoks = len(name_toks)
  firm_toks += zip([gvkey]*ntoks,range(ntoks),name_toks,[ntoks]*ntoks)
  if len(firm_toks) >= batch_size:
    cur_key.executemany(firmkey_cmd,firm_toks)
    del firm_toks[:]

  fnum += 1

# commit remainders
if len(firm_toks) > 0:
  cur_key.executemany(firmkey_cmd,firm_toks)

# create index
cur_key.execute("""create unique index firmkey_idx on firmkey(keyword asc, gvkey asc, idx asc)""")

# generate word frequencies
cur_key.execute("""drop table if exists keyword_freq""")
cur_key.execute("""drop table if exists firmkey2""")

cur_key.execute("""create table keyword_freq(keyword text, weight real)""")
cur_key.execute("""insert into keyword_freq select keyword,1.0/count(keyword) from firmkey group by keyword""")
cur_key.execute("""create table firmkey2(gvkey int, idx int, keyword text, ntoks int, weight real)""")
cur_key.execute("""insert into firmkey2 select firmkey.gvkey,firmkey.idx,firmkey.keyword,firmkey.ntoks,keyword_freq.weight from firmkey left outer join keyword_freq on (firmkey.keyword = keyword_freq.keyword)""")
cur_key.execute("""drop table firmkey""")
cur_key.execute("""alter table firmkey2 rename to firmkey""")
cur_key.execute("""create unique index firmkey_idx on firmkey(keyword asc, gvkey asc, idx asc)""")

cur_key.execute("""create table weight_total(gvkey int, wgt_tot real)""")
cur_key.execute("""insert into weight_total select gvkey,sum(weight) from firmkey group by gvkey""")
cur_key.execute("""create table firmkey2(gvkey int, idx int, keyword text, ntoks int, weight real, wgt_tot real)""")
cur_key.execute("""insert into firmkey2 select firmkey.gvkey,firmkey.idx,firmkey.keyword,firmkey.ntoks,firmkey.weight,weight_total.wgt_tot from firmkey left outer join weight_total on (firmkey.gvkey = weight_total.gvkey)""")
cur_key.execute("""drop table firmkey""")
cur_key.execute("""alter table firmkey2 rename to firmkey""")
cur_key.execute("""create unique index firmkey_idx on firmkey(keyword asc, gvkey asc, idx asc)""")

# close db
conn.commit()
conn.close()

print cnum
print fnum

