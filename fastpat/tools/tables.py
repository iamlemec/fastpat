# tools for patent data

import os
import pandas as pd

# csv types
dtypes = {
    'patnum': 'str',
    'appnum': 'str',
    'appdate': 'str',
    'pubdate': 'str',
    'execdate': 'str',
    'first_trans': 'str',
    'naics': 'Int64',
    'sic': 'Int64',
    'appname': 'str',
    'owner': 'str',
    'assignor': 'str',
    'assignee': 'str',
    'firm_num': 'Int64',
    'assignid': 'str',
    'bulkid': 'str',
    'id': 'Int64',
    'id1': 'Int64',
    'id2': 'Int64',
    'name': 'str',
    'name1': 'str',
    'name2': 'str',
    'src': 'str',
    'dst': 'str',
}

# read csv with proper types
def read_csv(fname, **kwargs):
    dt = {**dtypes, **kwargs.pop('dtype', {})}
    return pd.read_csv(fname, dtype=dt, **kwargs)

def astype(data, dtype):
    if dtype == 'str':
        return pd.Series(data, dtype='str')
    elif dtype == 'int':
        return pd.to_numeric(pd.Series(data), errors='coerce').astype('Int64')
    else:
        raise Exception(f'Unsupported type: {dtype}')

# insert in chunks
class ChunkWriter:
    def __init__(self, path, schema, chunk_size=1000, output=False):
        self.path = path
        self.schema = schema
        self.chunk_size = chunk_size
        self.output = output
        self.items = []
        self.i = 0
        self.j = 0

        self.file = open(self.path, 'w+')
        header = ','.join(schema)
        self.file.write(f'{header}\n')

    def __del__(self):
        self.file.close()

    def insert(self, *args):
        self.items.append(args)
        if len(self.items) >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def insertmany(self, args):
        self.items += args
        if len(self.items) >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def commit(self):
        self.i += 1
        self.j += len(self.items)

        if len(self.items) == 0:
            return

        if self.output:
            print(f'Committing chunk {self.i} to {self.table} ({len(self.items)})')

        data = [x for x in zip(*self.items)]
        frame = pd.DataFrame({k: astype(d, v) for (k, v), d in zip(self.schema.items(), data)})
        frame.to_csv(self.file, index=False, header=False)

        self.items.clear()

    def delete(self):
        self.file.close()
        os.remove(self.path)

# pretend to insert in chunks
class DummyWriter:
    def __init__(self, *args, chunk_size=1000, output=False, **kwargs):
        self.chunk_size = chunk_size
        self.output = output
        self.last = None
        self.i = 0

    def insert(self, *args):
        self.last = args
        self.i += 1
        if self.i >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def insertmany(self, args):
        if len(args) > 0:
            self.last = args[-1]
        self.i += len(args)
        if self.i >= self.chunk_size:
            self.commit()
            return True
        else:
            return False

    def commit(self):
        if self.output:
            print(self.last)
        self.i = 0

    def delete(self):
        pass
