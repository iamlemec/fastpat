import sys
import re
import sqlite3
import collections
import itertools

# postscripts
post0 = r"(\bA CORP.|;|,).*$"
post0_re = re.compile(post0)

# acronyms
acronym1 = r"\b(\w) (\w) (\w)\b"
acronym1_re = re.compile(acronym1)
acronym2 = r"\b(\w) (\w)\b"
acronym2_re = re.compile(acronym2)
acronym3 = r"\b(\w)-(\w)-(\w)\b"
acronym3_re = re.compile(acronym3)
acronym4 = r"\b(\w)-(\w)\b"
acronym4_re = re.compile(acronym4)
acronym5 = r"\b(\w\w)&(\w)\b"
acronym5_re = re.compile(acronym5)
acronym6 = r"\b(\w)&(\w)\b"
acronym6_re = re.compile(acronym6)
acronym7 = r"\b(\w) & (\w)\b"
acronym7_re = re.compile(acronym7)

# punctuation
punct0 = r"'S|\(.*\)|\."
punct1 = r"[^\w\s]"
punct0_re = re.compile(punct0)
punct1_re = re.compile(punct1)

# generic terms
states = ['DEL','DE','NY','VA','CA','PA','OH','NC','WI','MA']
compustat = ['PLC','CL','REDH','ADR','FD','LP','CP','TR','SP','COS','GP','OLD','NEW']
generics = ['THE','A','OF','AND','AN']
corps = ['CORPORATION','INCORPORATED','COMPANY','LIMITED','KABUSHIKI','KAISHA','AKTIENGESELLSCHAFT','AKTIEBOLAG','INC','LLC','LTD','CORP','AG','NV','BV','GMBH','CO','BV','SA','AB','SE']
typos = ['CORPORATIN','CORPORATON']
variants = ['TRUST','GROUP','GRP','HLDGS','HOLDINGS','COMM','INDS','COMM','HLDG','TECH','INTERNATIONAL']
dropout = states + compustat + generics + corps + typos + variants
gener_re = re.compile('|'.join([r"\b{}\b".format(el) for el in dropout]))

# standardize a firm name
def name_standardize_strong(name):
  name_strip = name
  name_strip = post0_re.sub('',name_strip)
  name_strip = acronym1_re.sub(r"\1\2\3",name_strip)
  name_strip = acronym2_re.sub(r"\1\2",name_strip)
  name_strip = acronym3_re.sub(r"\1\2\3",name_strip)
  name_strip = acronym4_re.sub(r"\1\2",name_strip)
  name_strip = acronym5_re.sub(r"\1\2",name_strip)
  name_strip = acronym6_re.sub(r"\1\2",name_strip)
  name_strip = acronym7_re.sub(r"\1\2",name_strip)
  name_strip = punct0_re.sub('',name_strip)
  name_strip = punct1_re.sub(' ',name_strip)
  name_strip = gener_re.sub('',name_strip)
  name_toks = name_strip.split()
  return name_toks

# actually store the data
store = True
fd
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

  if match:
    print '{:7}-{:7}, {:4.2}-{}: {:40.40} -> {:40.40}'.format(rowid,patnum,word_match,int(match),assignor,assignee)

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

