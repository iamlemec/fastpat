#!/usr/bin/python

from xml.sax import handler, make_parser, SAXException
import re
import sys
import sqlite3

# handle arguments
if len(sys.argv) <= 2:
  print 'Usage: parse_assign_sax.py filename store_db [num_lines]'
  sys.exit(0)

in_fname = sys.argv[1]
if sys.argv[2] == '1':
  store_db = True
else:
  store_db = False

if len(sys.argv) > 3:
  max_recs = int(sys.argv[3])
else:
  max_recs = sys.maxint

# detect if a string is a corporation name
corp_keys = ['CORP',' INC',' LLC','LTD','LIMITED','COMPANY','GMBH','KABUSHIKI','KAISHA',' AB\Z',' AG\Z',' SE\Z','B\.V\.','N\.V\.']
corp_re = re.compile('|'.join(corp_keys))
def is_corp(name):
  return corp_re.search(name) != None

# detect if a conveyance is not a name/address change or security agreement
assignment_keys = ['CHANGE','SECUR','CORRECT','RELEASE','LIEN','UPDATE','NUNC']
assignment_re = re.compile('|'.join(assignment_keys))
def is_assignment(conv):
  return assignment_re.search(conv) == None

# connect to assignment db
if store_db:
# connect to patent grant db
  db_fname = 'store/patents.db'
  assign_cmd = 'insert into assignment values (?,?,?,?,?,?)'
  conn = sqlite3.connect(db_fname)
  cur = conn.cursor()
  try:
    cur.execute('create table assignment (patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text)')
  except sqlite3.OperationalError as e:
    print e

# store for batch commit
batch_size = 1000
assignments = []

# SAX hanlder for patent assignments
class AssignmentHandler(handler.ContentHandler):
  def __init__(self):
    pass

  def startDocument(self):
    self.in_recd_date = False
    self.in_recd_date2 = False
    self.in_conveyance = False
    self.in_assignor = False
    self.in_assignee = False
    self.in_assignor_name = False
    self.in_assignee_name = False
    self.in_exec_date = False
    self.in_exec_date2 = False
    self.in_docnumber = False
    self.in_kind = False

    self.recs = 0
    self.pats = 0
    self.valid = 0
    self.multi_assign = 0

  def endDocument(self):
    pass

  def startElement(self, name, attrs):
    if name == 'patent-assignment':
      self.recd_date = ''
      self.exec_date = ''
      self.conveyance = ''
      self.assignee_name = ''
      self.assignor_name = ''
      self.patnum = ''
      self.patcand = ''
      self.patkind = ''
    elif name == 'recorded-date':
      self.in_recd_date = True
    elif name == 'conveyance-text':
      self.in_conveyance = True
      self.conveyance = ''
    elif name == 'patent-assignor':
      self.in_assignor = True
    elif name == 'patent-assignee':
      self.in_assignee = True
    elif name == 'name':
      if self.in_assignor:
        self.in_assignor_name = True
        self.assignor_name = ''
      elif self.in_assignee:
        self.in_assignee_name = True
        self.assignee_name = ''
    elif name == 'execution-date':
      self.in_exec_date = True
    elif name == 'date':
      if self.in_exec_date:
        self.in_exec_date2 = True
        self.exec_date = ''
      elif self.in_recd_date:
        self.in_recd_date2 = True
        self.recd_date = ''
    elif name == 'patent-property':
      self.patnum = ''
    elif name == 'doc-number':
      self.in_docnumber = True
      self.patcand = ''
    elif name == 'kind':
      self.in_kind = True
      self.patkind = ''

  def endElement(self, name):
    if name == 'patent-assignment':
      self.recs += 1
      if self.recs >= max_recs:
        raise SAXException('reached record limit')
    elif name == 'recorded-date':
      self.in_recd_date = False
    elif name == 'conveyance-text':
      self.in_conveyance = False
    elif name == 'patent-assignor':
      self.in_assignor = False
    elif name == 'patent-assignee':
      self.in_assignee = False
    elif name == 'name':
      self.in_assignor_name = False
      self.in_assignee_name = False
    elif name == 'execution-date':
      self.in_exec_date = False
    elif name == 'date':
      if self.in_recd_date2:
        self.in_recd_date2 = False
      elif self.in_exec_date2:
        self.in_exec_date2 = False
    elif name == 'patent-property':
      self.addPatent()
    elif name == 'document-id':
      if self.patnum == '' or self.patkind[0] == 'B':
        self.patnum = self.patcand
    elif name == 'doc-number':
      self.in_docnumber = False
    elif name == 'kind':
      self.in_kind = False

  def characters(self, content):
    if self.in_recd_date2:
      self.recd_date += content
    elif self.in_conveyance:
      self.conveyance += content
    elif self.in_assignor_name:
      self.assignor_name += content
    elif self.in_assignee_name:
      self.assignee_name += content
    elif self.in_exec_date2:
      self.exec_date += content
    elif self.in_docnumber:
      self.patcand += content
    elif self.in_kind:
      self.patkind += content

  def addPatent(self):
    self.pats += 1

    if self.patnum[0].isalpha() or not self.patkind.startswith('B'):
      return
    if not is_corp(self.assignor_name) or not is_corp(self.assignee_name) or not is_assignment(self.conveyance):
      return

    try:
      patint = int(self.patnum)
    except:
      return

    self.valid += 1

    #print '{:7},{:8},{:8},{: >20.20}: {: >30.30} -> {: <30.30}'.format(self.patnum,self.exec_date,self.recd_date,self.conveyance,self.assignor_name,self.assignee_name)

    # store in assign db
    if store_db:
      assignments.append((patint,self.exec_date,self.recd_date,self.conveyance,self.assignor_name,self.assignee_name))
      if len(assignments) >= batch_size:
        cur.executemany(assign_cmd,assignments)
        del assignments[:]

# do parsing
parser = make_parser()
assign_handler = AssignmentHandler()
parser.setContentHandler(assign_handler)
try:
  parser.parse(in_fname)
except SAXException as e:
  print e

# close db
if store_db:
  if len(assignments) > 0:
    cur.executemany(assign_cmd,assignments)

  conn.commit()
  conn.close()

# output stats
print in_fname
print assign_handler.recs
print assign_handler.pats
print assign_handler.valid

