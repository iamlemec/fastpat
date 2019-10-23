# tools for patent data

import pandas as pd

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
    'assignid': 'Int64',
    'id': 'Int64',
    'id1': 'Int64',
    'id2': 'Int64',
    'name': 'str',
    'name1': 'str',
    'name2': 'str',
    'src': 'str',
    'dst': 'str'
}

def read_csv(fname, **kwargs):
    dt = dtypes.copy()
    if 'dtype' in kwargs:
        dt.update(kwargs.pop('dtype'))
    return pd.read_csv(fname, dtype=dt, **kwargs)
