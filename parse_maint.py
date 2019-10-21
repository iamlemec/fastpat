import argparse
import sqlite3
import numpy as np
import pandas as pd

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO maintenance file parser.')
parser.add_argument('target', type=str, help='path of file to parse')
parser.add_argument('--output', type=str, default='tables', help='directory to store to')
args = parser.parse_args()

# import to dataframe
print('reading table')

colspec = [(0, 13), (14, 22), (23, 24), (25, 33), (34, 42), (43, 51), (52, 56)]
datf = pd.read_fwf(args.target, colspecs=colspec, usecols=[0, 2, 6], names=['patnum', 'is_small', 'event_code'])

# normalize patent number
datf['patnum'] = datf['patnum'].apply(lambda s: s.lstrip('0'))

# clean up data
print('data cleanup')

m4 = ['M1551', 'M170', 'M173', 'M183', 'M2551', 'M273', 'M283']
m8 = ['M1552', 'M171', 'M174', 'M184', 'M2552', 'M274', 'M284']
m12 = ['M1553', 'M172', 'M175', 'M185', 'M2553', 'M275', 'M285']
mmap = [(m, 4) for m in m4] + [(m, 8) for m in m8] + [(m, 12) for m in m12]
codes = pd.DataFrame(mmap, columns=['code', 'lag']).set_index('code')

datf = datf.join(codes, on='event_code', how='left').dropna()
datf = datf.drop('event_code', axis=1)
datf['is_small'] = datf['is_small'] == 'Y'
pat_groups = datf.groupby('patnum')
dpat = pd.DataFrame({
    'last_maint': pat_groups['lag'].max().astype(int),
    'ever_large': ~pat_groups['is_small'].min().astype(bool)
}).reset_index()

# write to disk
print('writing table')
dpat.to_csv(f'{args.output}/maint.csv', index=False)
