import sqlite3
import csv
from standardize import name_standardize

# file names
db_fname_nber = 'store/nber.db'

# connect to compustat db
conn = sqlite3.connect(db_fname_nber)
cur = conn.cursor()

# create tables
cur.execute('drop table if exists gv_keyword')
cur.execute('drop table if exists gv_name')
cur.execute('drop table if exists pdpass_gy')
cur.execute('drop table if exists assignee_name')
cur.execute('create table gv_keyword (gvkey int, idx int, keyword text, ntoks int)')
cur.execute('create table gv_name (gvkey int, name text)')
cur.execute('create table pdpass_gy (pdpass int, gvkey int, year int)')
cur.execute('create table assignee_name (pdp_num int, uspto_num int, name text)')

# sqlite insert commands
firmkey_cmd = 'insert into gv_keyword values (?,?,?,?)'
firmname_cmd = 'insert into gv_name values (?,?)'
pdpass_cmd = 'insert into pdpass_gy values (?,?,?)'
assignee_cmd = 'insert into assignee_name values (?,?,?)'

# open pdpco file
tsv_fname = 'nber_files/pdpcohdr.tsv'
tsv_fid = open(tsv_fname,'r')

# store for batch commit
batch_size = 1000
firm_toks = []
firm_names = []

# parse name->gvkey csv
firstLine = True
for line in tsv_fid:
  if firstLine:
    firstLine = False
    continue

  (name,_,_,gvkey,_,_,match,byr,eyr,_) = line.split('\t')

  if match == '-1':
    continue

  name = name.strip('\"')
  name_toks = name_standardize(name)

  #print '{:40.40}: {}'.format(name,', '.join(name_toks))

  ntoks = len(name_toks)
  firm_toks += zip([gvkey]*ntoks,range(ntoks),name_toks,[ntoks]*ntoks)
  firm_names += [(gvkey,name)]

  if len(firm_toks) >= batch_size:
    cur.executemany(firmkey_cmd,firm_toks)
    del firm_toks[:]
  if len(firm_names) >= batch_size:
    cur.executemany(firmname_cmd,firm_names)
    del firm_names[:]

# commit remainders
if len(firm_toks) > 0:
  cur.executemany(firmkey_cmd,firm_toks)
if len(firm_names) > 0:
  cur.executemany(firmname_cmd,firm_names)

# open dynass file
tsv_fname = 'nber_files/dynass.tsv'
tsv_fid = open(tsv_fname,'r')

# keep track of lines
dynass_gys = []

# parse (gvkey,year)->pdpass csv
firstLine = True
for line in tsv_fid:
  if firstLine:
    firstLine = False
    continue

  toks = line.split()
  ntoks = len(toks)
  nblocks = (ntoks-2)/4

  pdpass = toks[0]
  for b in range(nblocks):
    gv_key = toks[4+4*b]
    int_beg_yr = int(toks[3+4*b])
    int_end_yr = int(toks[5+4*b])
    n_yrs = int_end_yr-int_beg_yr+1
    dynass_gys += zip([pdpass]*n_yrs,[gv_key]*n_yrs,range(int_beg_yr,int_end_yr+1))

  if len(dynass_gys) >= batch_size:
    cur.executemany(pdpass_cmd,dynass_gys)
    del dynass_gys[:]

# commit remaining
if len(dynass_gys) > 0:
  cur.executemany(pdpass_cmd,dynass_gys)

# index for speed
cur.execute("""create unique index gv_keyword_idx on gv_keyword(keyword asc, gvkey asc, idx asc)""")
cur.execute("""create unique index gv_name_idx on gv_name(gvkey asc)""")
cur.execute("""create unique index gv_year_idx on pdpass_gy(pdpass asc, year asc)""")

# import assignee file
tsv_fname = 'nber_files/assignee.asc'
tsv_reader = csv.reader(open(tsv_fname,'rb'),delimiter='\t',quotechar='\"')
tsv_reader.next() # skip first line
assignees = [(pdp_num,uspto_num,unicode(name,errors='replace')) for (_,_,pdp_num,name,uspto_num) in tsv_reader]
cur.executemany(assignee_cmd,assignees)

# create index on name
cur.execute('create unique index assignee_name_idx on assignee_name(name asc)')

# close db
conn.commit()
conn.close()
