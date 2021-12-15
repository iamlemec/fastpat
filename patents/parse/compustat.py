import argparse
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='Compustat file parser.')
parser.add_argument('target', type=str, help='path of file to parse')
parser.add_argument('--output', type=str, default='tables', help='directory to store to')
args = parser.parse_args()

colmap = {
    'row': 'row',
    'gvkey': 'gvkey',
    'fyear': 'year',
    'conm': 'name',
    'AT': 'assets',
    'CAPX': 'capx',
    'CH': 'cash',
    'COGS': 'cogs',
    'CSHO': 'shares',
    'DP': 'deprec',
    'EBITDA': 'income',
    'EMP': 'employ',
    'INTAN': 'intan',
    'LT': 'debt',
    'REVT': 'revenue',
    'SALE': 'sales',
    'XRD': 'rnd',
    'XSGA': 'fcost',
    'prcc_f': 'price',
    'NAICS': 'naics',
    'SIC': 'sic'
}

dtype = {
    'gvkey': 'Int64',
    'year': 'Int64',
    'naics': 'Int64',
    'sic': 'Int64'
}

# read frame into memory
datf = pd.read_csv(args.target, error_bad_lines=False, index_col=0, usecols=colmap, dtype=dtype)
datf = datf.rename(columns=colmap)

# clean up data
datf['mktval'] = datf['shares']*datf['price']
datf = datf.drop(['shares', 'price'], axis=1)
datf = datf.dropna(subset=['gvkey', 'year', 'name'])
datf['name'] = datf['name'].str.lower()
datf['gvkey'] = datf['gvkey'].astype('Int64')
datf['year'] = datf['year'].astype('Int64')
datf['naics'] = datf['naics'].fillna(0).astype(int).map(lambda x: f'{x:<06d}')
datf['sic'] = datf['naics'].fillna(0).astype(int).map(lambda x: f'{x:<06d}')
datf = datf.reset_index(drop=True).rename_axis('compid').reset_index()

# write to disk
datf.to_csv(f'{args.output}/compustat.csv', index=False, float_format='%.3f')
