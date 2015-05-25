import sqlite3

stage = 3

# open db
assign_db = 'store/patents.db'
conn = sqlite3.connect(assign_db)
cur = conn.cursor()

if stage <= 0:
  print 'Creating primary key on patnum'

  # create primary key for patent
  cur.execute("""delete from patent where rowid not in (select min(rowid) from patent group by patnum)""")
  cur.execute("""create unique index patnum_idx on patent (patnum asc)""")

if stage <= 1:
  print 'Transforming patent dates'

  # tranform dates
  cur.execute("""update patent set filedate=substr(filedate,1,4)||'-'||substr(filedate,5,2)||'-'||substr(filedate,7,2) where filedate!=''""")
  cur.execute("""update patent set grantdate=substr(grantdate,1,4)||'-'||substr(grantdate,5,2)||'-'||substr(grantdate,7,2) where grantdate!=''""")
  cur.execute("""update assignment set execdate=substr(execdate,1,4)||'-'||substr(execdate,5,2)||'-'||substr(execdate,7,2) where execdate!=''""")
  cur.execute("""update assignment set recdate=substr(recdate,1,4)||'-'||substr(recdate,5,2)||'-'||substr(recdate,7,2) where recdate!=''""")

if stage <= 2:
  print 'Fixing errors'

  # remove blanks
  cur.execute("""delete from patent where filedate='' or grantdate=''""")
  cur.execute("""update patent set classone=-1 where typeof(classone)!='integer'""")
  cur.execute("""update patent set classtwo=-1 where typeof(classtwo)!='integer'""")
  cur.execute("""delete from assignment where execdate=''""")

  # special
  cur.execute("""update patent set filedate='1982-06-25' where patnum=4423140 and filedate='1298-06-25'""")
  cur.execute("""update patent set filedate='1987-04-13' where patnum=4739747 and filedate='1897-04-13'""")
  cur.execute("""update patent set filedate='1986-04-03' where patnum=4732727 and filedate='9186-04-03'""")
  cur.execute("""update patent set filedate='1978-07-21' where patnum=4198308 and filedate='7978-07-21'""")
  cur.execute("""update patent set filedate='1978-04-05' where patnum=4469216 and filedate='8198-04-05'""")
  cur.execute("""update assignment set execdate='2005-09-20' where patnum=5870562 and execdate='0205-09-20'""")

  # fix date typos
  cur.execute("""update patent set filedate=date(filedate,'+1000 years') where filedate<'1000-01-01'""")
  cur.execute("""update patent set filedate=date(filedate,'+900 years') where filedate<'1100-01-01' and filedate>='1000-01-01'""")
  cur.execute("""update patent set filedate=date(filedate,'+300 years') where filedate<'1700-01-01' and filedate>='1600-01-01'""")
  cur.execute("""update patent set filedate=date(filedate,'+100 years') where filedate<'1900-01-01' and filedate>='1800-01-01'""")
  cur.execute("""update patent set filedate=date(filedate,'-1000 years') where filedate<'3000-01-01' and filedate>='2900-01-01'""")
  cur.execute("""update patent set filedate=date(filedate,'-7200 years') where filedate<'9200-01-01' and filedate>='9100-01-01'""")

if stage <= 3:
  print 'Merging assignments and patents'

  # merge assignments with patents
  cur.execute("""drop table if exists assignment_pat""")
  cur.execute("""create table assignment_pat (assignid integer primary key asc, patnum int, filedate text, grantdate text, classone int, classtwo int, execdate text, recdate text, conveyance text, assignor text, assignee text, dup_flag int default 0)""")
  cur.execute("""insert into assignment_pat (patnum,filedate,grantdate,classone,classtwo,execdate,recdate,conveyance,assignor,assignee) select assignment.patnum,patent.filedate,patent.grantdate,patent.classone,patent.classtwo,assignment.execdate,assignment.recdate,assignment.conveyance,assignment.assignor,assignment.assignee from assignment left outer join patent on assignment.patnum = patent.patnum""")
  cur.execute("""delete from assignment_pat where filedate is null or grantdate is null or execdate=''""")

if stage <= 4:
  print 'Selecting only valid patents'

  cur.execute("""drop table if exists patent_use""")
  cur.execute("""create table patent_use as select * from patent where owner!=''""")

# close db
conn.commit()
conn.close()
