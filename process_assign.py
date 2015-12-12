import sys
import sqlite3
from name_standardize import name_standardize_strong

# actually store the data
store = True

# open db
db_fname = 'store/patents.db'
conn = sqlite3.connect(db_fname)
cur = conn.cursor()
cur_same = conn.cursor()
cmd_same = 'update assignment_pat set dup_flag=? where rowid=?'

batch_size = 1000
same_flags = []

rlim = sys.maxint
match_num = 0
rnum = 0
for row in cur.execute('select rowid,patnum,assignor,assignee,conveyance from assignment'):
  (rowid,patnum,assignor,assignee,conveyance) = row

  assignor_toks = name_standardize_strong(assignor)
  assignee_toks = name_standardize_strong(assignee)

  word_match = 0
  for tok in assignor_toks:
    if tok in assignee_toks:
      word_match += 1

  word_match /= max(1.0,0.5*(len(assignor_toks)+len(assignee_toks)))
  match = word_match > 0.5

  # if match:
  #   print '{:7}-{:7}, {:4.2}-{}: {:40.40} -> {:40.40}'.format(rowid,patnum,word_match,int(match),assignor,assignee)

  if store:
    same_flags.append((match,rowid))
    if len(same_flags) >= batch_size:
      cur_same.executemany(cmd_same,same_flags)
      del same_flags[:]

  match_num += match

  rnum += 1
  if rnum >= rlim:
    break

  if rnum%50000 == 0:
    print rnum

if store:
  # clean up
  if len(same_flags) > 0:
    cur_same.executemany(cmd_same,same_flags)

  # use the first entry that doesn't have same_flag=1
  cur.execute('drop table if exists assignment_use')
  cur.execute('create table assignment_use as select * from assignment_pat where rowid in (select min(rowid) from assignment_pat group by patnum,execdate,dup_flag) and dup_flag=0 and execdate!=\'\'')
  cur.execute('create unique index assign_idx on assignment_use(patnum,execdate)')

  # commit changes
  conn.commit()

# close db
conn.close()

print match_num
print rnum
print rnum-match_num
print float(match_num)/float(rnum)
