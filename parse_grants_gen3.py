#!/usr/bin/python

from xml.sax import handler, make_parser, SAXException
import sys
import sqlite3

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
    cur.execute("create table patent (patnum int, filedate text, grantdate text, classone int, classtwo int, ipcver text, ipccode text, owner text)")
  except sqlite3.OperationalError as e:
    print e

# store for batch commit
batch_size = 1000
patents = []

def commitBatch():
  if store_db:
    cur.executemany('insert into patent values (?,?,?,?,?,?,?,?)',patents)
  del patents[:]

# SAX hanlder for gen3 patent grants
class GrantHandler(handler.ContentHandler):
  def __init__(self):
    pass

  def startDocument(self):
    self.in_pubref = False
    self.in_appref = False
    self.in_patnum = False
    self.in_grantdate = False
    self.in_filedate = False
    self.in_fos = False
    self.in_classnat = False
    self.in_mainclass = False
    self.in_assignee = False
    self.in_orgname = False

    self.in_ipc = False
    self.in_ipc_version = False
    self.in_ipc_section = False
    self.in_ipc_class = False
    self.in_ipc_subclass = False
    self.in_ipc_group = False
    self.in_ipc_subgroup = False

    self.completed = 0

  def endDocument(self):
    pass

  def startElement(self, name, attrs):
    if name == 'us-patent-grant':
      self.patnum = ''
      self.grant_date = ''
      self.file_date = ''
      self.class_str = ''
      self.ipc_ver = ''
      self.ipc_code = ''
      self.orgname = ''
    elif name == 'publication-reference':
      self.in_pubref = True
    elif name == 'application-reference':
      self.in_appref = True
    elif name == 'field-of-search':
      self.in_fos = True
    elif name == 'doc-number':
      if self.in_pubref:
        self.in_patnum = True
    elif name == 'classification-national':
      if not self.in_fos:
        self.in_classnat = True
    elif name == 'main-classification':
      if self.in_classnat:
        self.in_mainclass = True
    elif name == 'classification-ipcr':
      self.in_ipc = True
      self.ipc_ver = ''
      self.ipc_code = ''
    elif name == 'ipc-version-indicator':
      if self.in_ipc:
        self.in_ipc_version = True
    elif name == 'section':
      if self.in_ipc:
        self.in_ipc_section = True
    elif name == 'class':
      if self.in_ipc:
        self.in_ipc_class = True
    elif name == 'subclass':
      if self.in_ipc:
        self.in_ipc_subclass = True
    elif name == 'main-group':
      if self.in_ipc:
        self.in_ipc_group = True
    elif name == 'subgroup':
      if self.in_ipc:
        self.in_ipc_subgroup = True
    elif name == 'date':
      if self.in_pubref:
        self.in_grantdate = True
      elif self.in_appref:
        self.in_filedate = True
    elif name == 'assignee':
      if self.orgname == '':
        self.in_assignee = True
    elif name == 'orgname':
      if self.in_assignee:
        self.in_orgname = True

  def endElement(self, name):
    if name == 'us-patent-grant':
      if self.patnum[0] == '0':
        self.addPatent()
    elif name == 'publication-reference':
      self.in_pubref = False
    elif name == 'application-reference':
      self.in_appref = False
    elif name == 'field-of-search':
      self.in_fos = False
    elif name == 'doc-number':
      self.in_patnum = False
    elif name == 'classification-national':
      if not self.in_fos:
        self.in_classnat = False
    elif name == 'main-classification':
      if self.in_classnat:
        self.in_mainclass = False
    elif name == 'classification-ipcr':
      self.in_ipc = False
    elif name == 'ipc-version-indicator':
      if self.in_ipc:
        self.in_ipc_version = False
    elif name == 'section':
      if self.in_ipc:
        self.in_ipc_section = False
    elif name == 'class':
      if self.in_ipc:
        self.in_ipc_class = False
    elif name == 'subclass':
      if self.in_ipc:
        self.in_ipc_subclass = False
    elif name == 'main-group':
      if self.in_ipc:
        self.in_ipc_group = False
    elif name == 'subgroup':
      if self.in_ipc:
        self.in_ipc_subgroup = False
    elif name == 'date':
      if self.in_grantdate:
        self.in_grantdate = False
      elif self.in_filedate:
        self.in_filedate = False
    elif name == 'assignee':
      self.in_assignee = False
    elif name == 'orgname':
      self.in_orgname = False

  def characters(self, content):
    if self.in_patnum:
      self.patnum += content
    if self.in_grantdate:
      self.grant_date += content
    if self.in_filedate:
      self.file_date += content
    if self.in_mainclass:
      self.class_str += content
    if self.in_orgname:
      self.orgname += content
    if self.in_ipc:
      if self.in_ipc_version:
        self.ipc_ver += content
      elif self.in_ipc_section or self.in_ipc_class or self.in_ipc_subclass or self.in_ipc_group:
        self.ipc_code += content
      elif self.in_ipc_subgroup:
        self.ipc_code += '/' + content

  def addPatent(self):
    self.completed += 1

    self.patint = self.patnum[1:]
    self.class_one = self.class_str[:3]
    self.class_two = self.class_str[3:6]
    self.ipc_ver = self.ipc_ver.strip()
    self.orgname_esc = self.orgname.encode('ascii','ignore').upper()

    if not store_db: print '{:7} {} {} {:.3} {:.3} {:.8} {:9.9} {:.30}'.format(self.patint,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_ver,self.ipc_code,self.orgname_esc)

    patents.append((self.patint,self.file_date,self.grant_date,self.class_one,self.class_two,self.ipc_ver,self.ipc_code,self.orgname_esc))
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


