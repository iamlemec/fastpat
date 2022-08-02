# match citation data with aggregated firm data (to be run before firm merge)

import os
import numpy as np
import pandas as pd

from ..tools.tables import read_csv

# match and aggregates cites
def aggregate_chunk(cites, grants):
    print(len(cites))

    # match citations to firms with patnum
    cites = cites.rename(columns={'src': 'citer_pnum', 'dst': 'citee_pnum'})
    cites = cites.join(grants.add_prefix('citer_'), on='citer_pnum')
    cites = cites.join(grants.add_prefix('citee_'), on='citee_pnum')
    cites['self_cite'] = (cites['citer_firm_num'] == cites['citee_firm_num']).fillna(False)

    # patent level statistics
    stats = pd.DataFrame({
        'n_cited': cites.groupby('citer_pnum').size(),
        'n_citing': cites.groupby('citee_pnum').size(),
        'n_self_cited': cites.groupby('citer_pnum')['self_cite'].sum(),
        'n_self_citing': cites.groupby('citee_pnum')['self_cite'].sum(),
    }).rename_axis(index='patnum')
    stats = stats.fillna(0).astype(np.int)

    return stats

def aggregate_cites(output, chunksize=10000000):
    grant_path = os.path.join(output, 'grant_firm.csv')
    cite_path = os.path.join(output, 'grant_cite.csv')
    stats_path = os.path.join(output, 'cite_stats.csv')

    # load in grant data
    grants = read_csv(grant_path).set_index('patnum')
    request = read_csv(cite_path, chunksize=chunksize)

    # loop over citation chunks (otherwise requires >32GB of RAM)
    cite_stats = pd.concat([aggregate_chunk(df, grants) for df in request], axis=0)
    cite_stats = cite_stats.groupby('patnum').sum() # since patents can span multiple chunks
    cite_stats.to_csv(stats_path)
