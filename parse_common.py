from xml.sax import handler
from lxml import etree

# preserve memory
def clear(elem):
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]

def get_text(elem,default=''):
    return (elem.text if elem.text is not None else default) if elem is not None else default

# demangle file
def parse_mangled_gen3(fname,handler):
    pp = etree.XMLPullParser(tag='us-patent-grant', events=['end'])
    def parse_all():
        for (_, pat) in pp.read_events():
            if not handler(pat):
                return False
        return True

    with open(fname) as f:
        pp.feed('<root>\n')
        for line in f:
            if line.startswith('<?xml'):
                if not parse_all():
                    break
            elif line.startswith('<!DOCTYPE'):
                pass
            else:
                pp.feed(line)
        else:
            pp.feed('</root>\n')
            parse_all()

# insert in chunks
class ChunkInserter:
    def __init__(self,con,table=None,cmd=None,cur=None,chunk_size=1000,output=False):
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
            self.cmd = 'insert into %s values (%s)' % (self.table, sign)
        if self.output:
            print('Committing chunk %d (%d)' % (self.i,len(self.items)))
        self.cur.executemany(self.cmd,self.items)
        self.con.commit()
        self.items = []

# SAX handler - track xml path, no attributes
class PathHandler(handler.ContentHandler):
  def __init__(self,track_keys=[],start_keys=[],end_keys=[]):
    self.track_keys = track_keys
    self.start_keys = start_keys
    self.end_keys = end_keys

  def startDocument(self):
    self.path = []

  def endDocument(self):
    pass

  def startElement(self,name,attrs):
    if name in self.track_keys:
      self.path.append(name)

  def endElement(self,name):
    if name in self.track_keys and self.path and self.path[-1] == name:
      self.path.pop()
