import os
import glob
import numpy as np
import pandas as pd

# maint file layout
colspec = [(0, 13), (14, 22), (23, 24), (25, 33), (34, 42), (43, 51), (52, 56)]

# event code mapping
m4 = ['M1551', 'M170', 'M173', 'M183', 'M2551', 'M273', 'M283']
m8 = ['M1552', 'M171', 'M174', 'M184', 'M2552', 'M274', 'M284']
m12 = ['M1553', 'M172', 'M175', 'M185', 'M2553', 'M275', 'M285']
mmap = [(m, 4) for m in m4] + [(m, 8) for m in m8] + [(m, 12) for m in m12]
codes = pd.DataFrame(mmap, columns=['code', 'lag']).set_index('code')

def parse_file(fpath, output, overwrite=False, dryrun=False):
    fdir, fname = os.path.split(fpath)
    opath = os.path.join(output, 'maint_maint.csv')

    if not overwrite and os.path.exists(opath):
        print(f'{fname}: Skipping')
        return
    else:
        print(f'{fname}: Starting')

    # import to dataframe
    print('Reading table')
    datf = pd.read_fwf(
        fpath, colspecs=colspec, usecols=[0, 2, 6], names=['patnum', 'is_small', 'event_code']
    )
    datf['patnum'] = datf['patnum'].apply(lambda s: s.lstrip('0').lower())

    # clean up data
    print('Data cleanup')
    datf = datf.join(codes, on='event_code', how='left').dropna()
    datf = datf.drop('event_code', axis=1)
    datf['is_small'] = datf['is_small'] == 'Y'
    pat_groups = datf.groupby('patnum')
    dpat = pd.DataFrame({
        'last_maint': pat_groups['lag'].max().astype(int),
        'ever_large': ~pat_groups['is_small'].min().astype(bool)
    }).reset_index()

    # write to disk
    print('Writing table')
    if not dryrun:
        dpat.to_csv(opath, index=False)

def get_date(fpath):
    fdir, fname = os.path.split(fpath)
    fbase, _ = os.path.splitext(fname)
    _, date = fbase.split('_')
    return int(date)

# really this is only one file
def parse_many(files, output, overwrite=False, dryrun=False, threads=None):
    if os.path.isdir(files):
        # get latest file
        maint_files = glob.glob(f'{files}/MaintFeeEvents_*.txt')
        dates = [get_date(fp) for fp in maint_files]
        max_index = np.argmax(dates)
        file_one = maint_files[max_index]
    else:
        file_one = files

    # ensure output dir
    if not dryrun and not os.path.exists(output):
        print(f'Creating directory {output}')
        os.makedirs(output)

    parse_file(file_one, output, overwrite=overwrite, dryrun=dryrun)
