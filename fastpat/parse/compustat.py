import os
import pandas as pd

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

def parse_file(fpath, output, display=0, overwrite=False, dryrun=False):
    fdir, fname = os.path.split(fpath)
    opath = os.path.join(output, 'compustat_compustat.csv')

    if not overwrite and os.path.exists(opath):
        print(f'{fname}: Skipping')
        return
    else:
        print(f'{fname}: Starting')

    # read frame into memory
    datf = pd.read_csv(fpath, index_col=0, usecols=colmap, dtype=dtype)
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
    if not dryrun:
        datf.to_csv(opath, index=False, float_format='%.3f')

# really this is only one file
def parse_many(files, output, overwrite=False, dryrun=False, threads=None):
    if os.path.isdir(files):
        file_one = os.path.join(files, 'compustat.csv')
    else:
        file_one = files

    # ensure output dir
    if not dryrun and not os.path.exists(output):
        print(f'Creating directory {output}')
        os.makedirs(output)

    parse_file(file_one, output, overwrite=overwrite, dryrun=dryrun)
