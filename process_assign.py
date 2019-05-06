import argparse
import sqlite3
from standardize import standardize_strong

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO assign fixer.')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
args = parser.parse_args()

# detect same entity transfers
def same_entity(assignor, assignee):
    assignor_toks = standardize_strong(assignor)
    assignee_toks = standardize_strong(assignee)

    word_match = 0
    for tok in assignor_toks:
        if tok in assignee_toks:
            word_match += 1

    word_match /= max(1.0, 0.5*(len(assignor_toks)+len(assignee_toks)))
    match = word_match > 0.5
    return match

# map to two-digit country codes
country_map {

}

# open database
with sqlite3.connect(args.db) as con:
    assn = pd.read_sql('select * from assign', con)
    assn['same'] = assn[['assignor', 'assignee']].apply(same_entity)
    good = assn[~assn['same']].drop('same', axis=1)
    good.to_sql('assign_use', con, index=False, if_exists='replace')
