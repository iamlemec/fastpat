import sys
import sqlite3
from name_standardize import name_standardize_strong

# actually store the data
store = True

# open db
if store:
    db_fname = 'store/patents.db'
    con = sqlite3.connect(db_fname)
    cur = con.cursor()
    cur_ins = con.cursor()

    # create table
    cur_ins.execute('create table assignment_use (assignid integer primary key, patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text, assignee_state text, assignee_country text)')
    cmd_ins = 'insert into assignment_use values (?,?,?,?,?,?,?,?,?)'

# batch insertion
batch_size = 10000
assignments = []

rlim = sys.maxsize
match_num = 0
rnum = 0
for row in cur.execute('select rowid,* from assignment'):
    (assignee,assignor) = (row[5],row[6])

    assignor_toks = name_standardize_strong(assignor)
    assignee_toks = name_standardize_strong(assignee)

    word_match = 0
    for tok in assignor_toks:
        if tok in assignee_toks:
            word_match += 1

    word_match /= max(1.0,0.5*(len(assignor_toks)+len(assignee_toks)))
    match = word_match > 0.5

    # if match:
    #   print('{:7}-{:7}, {:4.2}-{}: {:40.40} -> {:40.40}'.format(rowid,patnum,word_match,int(match),assignor,assignee))

    if store:
        assignments.append(row)
        if len(assignments) >= batch_size:
            cur_ins.executemany(cmd_ins,assignments)
            del assignments[:]

    match_num += match

    rnum += 1
    if rnum >= rlim:
        break

    if rnum%50000 == 0:
      print(rnum)

# clean up
if store:
    if len(assignments) > 0:
        cur_ins.executemany(cmd_ins,assignments)
        del assignments[:]

    # for tracking ownership
    cur_ins.execute('delete from assignment_use where rowid not in (select max(rowid) from assignment_use group by patnum,execdate)')
    cur_ins.execute('create unique index assign_idx on assignment_use(patnum,execdate)')

    # commit changes
    con.commit()

# close db
con.close()

print(match_num)
print(rnum)
print(rnum-match_num)
print(float(match_num)/float(rnum))
