import sys
import sqlite3

# file names
db_fname_nber = 'store/nber.db'

# connect to compustat db
conn = sqlite3.connect(db_fname_nber)
cur = conn.cursor()

# create tables
cur.execute('drop table if exists grant')
cur.execute('create table grant (patnum int, pdpass int, fileyear int)')

# sqlite insert commands
grant_cmd = 'insert into grant values (?,?,?)'

# open pdpco file
tsv_fname = '../nber_patproj/pat76_06_assg.asc'
tsv_fid = open(tsv_fname,'r')

# store for batch commit
batch_size = 1000
grants = []

rec_lim = sys.maxint
nrec = 0

# parse name->gvkey csv
firstLine = True
for line in tsv_fid:
  if firstLine:
    firstLine = False
    continue

  toks = line.split('\t')
  fileyear = toks[1]
  patnum = toks[20]
  pdpass = toks[21]

  #print '{:7} {:4}: {:8}'.format(patnum,fileyear,pdpass)

  grants.append((patnum,pdpass,fileyear))
  if len(grants) >= batch_size:
    cur.executemany(grant_cmd,grants)
    del grants[:]

  nrec += 1
  if nrec > rec_lim:
    break

  if nrec % 500000 == 0:
    print nrec

# commit remainders
if len(grants) > 0:
  cur.executemany(grant_cmd,grants)

# match with pdpass_gy
cur.execute('drop table if exists matched_grant')
cur.execute('create table matched_grant (patnum int, pdpass int, fileyear int, gvkey int)')
cur.execute('insert into matched_grant select grant.patnum,grant.pdpass,grant.fileyear,pdpass_gy.gvkey from grant left outer join pdpass_gy on (grant.fileyear = pdpass_gy.year and grant.pdpass = pdpass_gy.pdpass)')
print cur.execute('select count(*) from matched_grant where gvkey not null')[0][0]

# close db
conn.commit()
conn.close()
