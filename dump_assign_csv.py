#!/usr/bin/python

import sqlite3

db_fname = 'store/patents.db'
csv_fname = 'store/assignments.csv'

conn = sqlite3.connect(db_fname)
cur = conn.cursor()

outp = open(csv_fname,'w')

ret = cur.execute('select * from assignment')
for row in ret:
  row_unicode = u'\"{}\"\n'.format('\",\"'.join([unicode(el) for el in row]))
  outp.write(row_unicode.encode('ascii','ignore'))

conn.close()

