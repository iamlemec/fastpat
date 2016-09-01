from xml.sax import handler
from lxml import etree

# preserve memory
def clear(elem):
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]

def get_text(elem,default=''):
    return (elem.text if elem.text is not None else default) if elem is not None else default

def create_patent_table(con):
    cur = con.cursor()
    cur.execute('create table if not exists patent (patnum int, filedate text, grantdate text, ipcver text, ipcclass text, ipcgroup, state text, country text, owner text)')

# parser, emulate SAX here
class ParserGen1:
  def __init__(self):
      pass

  def setContentHandler(self,handler):
      self.handler = handler

  def parse(self,fname):
      fid = open(fname,encoding='ISO-8859-1')
      for line in fid:
          line = line[:-1]

          if len(line) == 0 or line[0] == ' ':
              continue

          tag = line[:4].strip()
          text = line[5:]

          if not self.handler.tag(tag.rstrip(),text):
              break

# demangle file, emulate SAX
class ParserGen2:
    def __init__(self):
        pass

    def setContentHandler(self, handler):
        self.handler = handler

    def parse(fname):
        pp = etree.XMLPullParser(tag='PATDOC', events=['end'], recover=True)

        def handle_all():
            for (_, pat) in pp.read_events():
                if not handler(pat):
                    return False
                clear(pat)
            return True

        with open(fname) as f:
            pp.feed('<root>\n')
            for line in f:
                if line.startswith('<?xml'):
                    if not handle_all():
                        break
                elif line.startswith('<!DOCTYPE') or line.startswith('<!ENTITY') or line.startswith(']>'):
                    pass
                else:
                    pp.feed(line)
            else:
                pp.feed('</root>\n')
                handle_all()

# demangle file
class ParserGen3:
    def __init__(self):
        pass

    def setContentHandler(self, handler):
        self.handler = handler

    def parse(fname):
        pp = etree.XMLPullParser(tag='us-patent-grant', events=['end'], recover=True)

        def handle_all():
            for (_, pat) in pp.read_events():
                if not self.handler(pat):
                    return False
                clear(pat)
            return True

        with open(fname) as f:
            pp.feed('<root>\n')
            for line in f:
                if line.startswith('<?xml'):
                    if not handle_all():
                        break
                elif line.startswith('<!DOCTYPE'):
                    pass
                else:
                    pp.feed(line)
            else:
                pp.feed('</root>\n')
                handle_all()

# insert in chunks
class ChunkInserter:
    def __init__(self, con, table=None, cmd=None, cur=None, chunk_size=1000, output=False):
        if table is None and cmd is None:
            raise('Must specify either table or cmd')

        self.con = con
        self.cur = cur if cur is not None else con.cursor()
        self.table = table
        self.cmd = cmd
        self.chunk_size = chunk_size
        self.output = output
        self.items = []
        self.i = 0

    def insert(self,*args):
        self.items.append(args)
        if len(self.items) >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def insertmany(self,args):
        self.items += args
        if len(self.items) >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def commit(self):
        self.i += 1
        if len(self.items) == 0:
            return
        if self.cmd is None:
            nargs = len(self.items[0])
            sign = ','.join(nargs*'?')
            self.cmd = 'insert or replace into %s values (%s)' % (self.table, sign)
        if self.output:
            print('Committing chunk %d (%d)' % (self.i, len(self.items)))
        self.cur.executemany(self.cmd, self.items)
        self.con.commit()
        self.items = []
