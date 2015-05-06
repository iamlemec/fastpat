#!/usr/bin/python

import sys
import sqlite3
from xml.sax import make_parser
from parse_grants_common import PathHandler

# handle arguments
if len(sys.argv) <= 1:
  print 'Usage: parse_grants_gen3.py filename store_db'
  sys.exit(0)

in_fname = sys.argv[1]
if sys.argv[2] == '1':
  store_db = True
else:
  store_db = False

if store_db:
  # database file
  db_fname = 'store/patents.db'
  conn = sqlite3.connect(db_fname)
  cur = conn.cursor()
  try:
    cur.execute("create table patent (patnum int, filedate text, grantdate text, classone int, classtwo int, ipcver text, ipccode text, city text, country text, owner text)")
  except sqlite3.OperationalError as e:
    print e

# store for batch commit
batch_size = 1000
patents = []

def commitBatch():
  if store_db:
    cur.executemany('insert into patent values (?,?,?,?,?,?,?,?,?,?)',patents)
  del patents[:]

def forceUpper(s):
  return s.encode('ascii','ignore').upper()

# SAX hanlder for gen3 patent grants
class GrantHandler(PathHandler):
  def __init__(self):
    track_keys = ['us-patent-grant','publication-reference','application-reference','doc-number',
                 'classification-national','main-classification','classification-ipcr','ipc-version-indicator',
                 'section','class','subclass','main-group','subgroup','date','assignee','orgname','country','city']
    start_keys = ['us-patent-grant','classification-ipcr','assignee']
    end_keys = ['us-patent-grant','classification-ipcr','assignee']
    PathHandler.__init__(self,track_keys=track_keys,start_keys=start_keys,end_keys=end_keys)

    self.completed = 0

  def startElement(self,name,attrs):
    PathHandler.startElement(self,name,attrs)

    if name == 'us-patent-grant':
      self.patnum = ''
      self.grant_date = ''
      self.file_date = ''
      self.class_str = ''

      self.orgnames = []
      self.countries = []
      self.cities = []
      self.ipc_vers = []
      self.ipc_codes = []
    elif name == 'classification-ipcr':
      self.ipc_ver = ''
      self.ipc_code = ''
    elif name == 'assignee':
      self.orgname = ''
      self.country = ''
      self.city = ''

  def endElement(self,name):
    PathHandler.endElement(self,name)

    if name == 'us-patent-grant':
      if self.patnum[0] == '0':
        self.addPatent()
    elif name == 'classification-ipcr':
      self.ipc_vers.append(self.ipc_ver)
      self.ipc_codes.append(self.ipc_code)
    elif name == 'assignee':
      self.orgnames.append(self.orgname)
      self.countries.append(self.country)
      self.cities.append(self.city)

  def characters(self,content):
    if len(self.path) < 2:
      return

    if self.path[-2] == 'publication-reference':
      if self.path[-1] == 'doc-number':
        self.patnum += content
      elif self.path[-1] == 'date':
        self.grant_date += content
    elif self.path[-2] == 'application-reference' and self.path[-1] == 'date':
      self.file_date += content
    elif self.path[-2] == 'classification-national' and self.path[-1] == 'main-classification':
      self.class_str += content
    elif self.path[-2] == 'assignee':
      if self.path[-1] == 'orgname':
        self.orgname += content
      elif self.path[-1] == 'country':
        self.country += content
      elif self.path[-1] == 'city':
        self.city += content
    elif self.path[-2] == 'classification-ipcr':
      if self.path[-1] in ['section','class','subclass']:
        self.ipc_code += content
      elif self.path[-1] == 'subgroup':
        self.ipc_code += '/' + content

    if len(self.path) < 3:
      return

    if self.path[-3] == 'classification-ipcr':
      if self.path[-2] == 'ipc-version-indicator' and self.path[-1] == 'date':
        self.ipc_ver += content

  def addPatent(self):
    self.completed += 1

    self.patint = self.patnum[1:]

    self.class_one = self.class_str[:3].strip()
    self.class_two = self.class_str[3:6].strip()

    self.ipc_ver = self.ipc_vers[0] if self.ipc_vers else ''
    self.ipc_code = ','.join(self.ipc_codes)

    us_orgs = filter(lambda (i,s): s == 'US',enumerate(self.countries))
    org_idx = us_orgs[0][0] if us_orgs else 0
    self.orgname = self.orgnames[org_idx] if self.orgnames else ''
    self.country = self.countries[org_idx] if self.countries else ''
    self.city = self.cities[org_idx] if self.cities else ''

    self.city = forceUpper(self.city)
    self.orgname = forceUpper(self.orgname)

    if not store_db: print '{:7} {} {} {:3.3} {:3.3} {:9.9} {:3} {:15} {:3} {:.30}'.format(self.patint,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_codes[0],len(self.ipc_codes),self.city,self.country,self.orgname)

    patents.append((self.patint,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_ver,self.ipc_code,self.city,self.country,self.orgname))
    if len(patents) == batch_size:
      commitBatch()

# do parsing
parser = make_parser()
grant_handler = GrantHandler()
parser.setContentHandler(grant_handler)
parser.parse(in_fname)

# clear out the rest
if len(patents) > 0:
  commitBatch()

if store_db:
  # commit to db and close
  conn.commit()
  cur.close()
  conn.close()

print grant_handler.completed
