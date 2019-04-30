import argparse
import sqlite3
from standardize import standardize_strong
from parse_tools import ChunkInserter

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO assign fixer.')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
args = parser.parse_args()

# detect organization type
ORG_CORP = 0
ORG_NONP = 1
ORG_INDV = 2

corp_keys = ['CORP', 'CO', 'INC', 'LLC', 'LP', 'PLC', 'LTD', 'LIMITED', 'COMPANY', 'CORPORATION', 'INCORPORATED', 'INTERNATIONAL', 'SYSTEMS', 'SA', 'OY', 'CONSULTING', 'BANK', 'GMBH', 'KABUSHIKI', 'KAISHA', 'BV', 'NV', 'SL', 'AKTIENGESELLSCHAFT', 'MASCHINENFABRIK', 'AB', 'AG', 'AS', 'SPA', 'HF', 'SOCIETE', 'ASSOCIATES', 'BUSINESS', 'INDUSTRIES', 'GROUP', 'KK', 'LABORATORIES', 'WORKS', 'STUDIO', 'TELECOM', 'INVESTMENTS', 'CONSULTANTS', 'ELECTRONICS', 'TECHNOLOGIES', 'MICROSYSTEMS', 'MULTIMEDIA', 'NETWORKS', 'TECHNOLOGY', 'PARTNERSHIP', 'ELECTRIC', 'COMPONENTS', 'AUTOMOTIVE', 'INSTRUMENTS', 'COMMUNICATION', 'ENTERPRISES', 'NETWORK', 'ENGINEERING', 'DESIGNS', 'SCIENCES', 'PARTNERS', 'AKTIENGELLSCHAFT', 'VENTURE', 'AEROSPACE', 'PHARMACEUTICALS', 'DESIGN', 'MEDICAL', 'PRODUCTS', 'PHARMA', 'ENERGY', 'SOLUTIONS', 'FRANCE', 'ISREAL', 'PRODUCT', 'PLASTICS', 'COMMUNICATIONS', 'KGAA', 'SAS', 'CELLULAR', 'GESELLSCHAFT', 'SE', 'HOLDINGS', 'KG', 'SRL', 'CHIMIE']

nonp_keys = ['INSTITUTE', 'UNIVERSITY', 'HOSPITAL', 'FOUNDATION', 'COLLEGE', 'RESEARCH', 'ADMINISTRATION', 'RECHERCHE', 'DEPARTMENT', 'TRUST', 'ASSOCIATION', 'MINISTRY', 'LABORATORY', 'BOARD', 'OFFICE', 'UNIV', 'ECOLE', 'SECRETARY', 'UNIVERSIDAD', 'SOCIETY', 'UNIVERSITEIT', 'CENTRE', 'CENTER', 'NATIONAL', 'SCHOOL', 'INSTITUT', 'INSTITUTES', 'UNIVERSITE']

punc_re = re.compile(r'[0-9&()]')
corp_re = re.compile('\\b('+'|'.join(corp_keys)+')\\b')
nonp_re = re.compile('\\b('+'|'.join(nonp_keys)+')\\b')

def org_type(name):
    name = name.replace('.', '')
    name = name.replace('/', '')
    has_comma = name.find(',') != -1
    has_corp = corp_re.search(name) != None
    has_nonp = nonp_re.search(name) != None
    has_punc = punc_re.search(name) != None
    has_space = name.find(' ') != -1
    if has_corp or has_punc or not has_space:
        return ORG_CORP
    elif has_nonp:
        return ORG_NONP
    else:
        return ORG_INDV

# detect conveyance type
CONV_ASSIGN = 0
CONV_LICENSE = 1
CONV_MERGER = 2
CONV_OTHER = 3

# detect if a conveyance is not a name/address change or security agreement
other_keys = ['CHANGE', 'SECUR', 'CORRECT', 'RELEASE', 'LIEN', 'UPDATE', 'NUNC', 'COLLAT']
other_re = re.compile('|'.join(other_keys))

def convey_type(convey):
    if other_re.search(convey) != None:
        return CONV_OTHER
    elif convey.find('ASSIGN') != -1:
        return CONV_ASSIGN
    elif convey.find('LICENSE') != -1:
        return CONV_LICENSE
    elif convey.find('MERGE') != -1:
        return CONV_MERGER
    else:
        return CONV_OTHER

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

# open database
con = sqlite3.connect(args.db)
cur = con.cursor()

# create table
cur.execute('DROP TABLE IF EXISTS assign_use')
cur.execute('CREATE TABLE assign_use (assignid integer primary key, patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text, assignee_state text, assignee_country text)')
chunker = ChunkInserter(con, table='assign_use')

######

# code names
src_type = org_type(assignor_name)
dst_type = org_type(assignee_name)
ctype = convey_type(convey)

# throw out individuals
if src_type == ORG_INDV or dst_type == ORG_INDV or ctype == CONV_OTHER:
    o += 1
    continue
