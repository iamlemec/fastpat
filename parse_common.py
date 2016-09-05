# common parsing tools

# get descendent text
def get_text(parent,tag,default=''):
    child = parent.find(tag)
    return (child.text or default) if child is not None else default

# get all text of node
def raw_text(par, sep=''):
    return sep.join(par.itertext()).strip()

# preserve memory
def clear(elem):
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]

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
