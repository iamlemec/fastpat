import re
import sys
from lxml.etree import iterparse, tostring
import sqlite3

# handle arguments
if len(sys.argv) <= 2:
    print('Usage: parse_assign_sax.py filename store_db [num_lines]')
    sys.exit(0)

in_fname = sys.argv[1]
if sys.argv[2] == '1':
    store_db = True
else:
    store_db = False

if len(sys.argv) > 3:
    max_recs = int(sys.argv[3])
else:
    max_recs = sys.maxsize

# detect organization type
ORG_CORP = 0
ORG_NONP = 1
ORG_INDV = 2

corp_keys = ['CORP','CO','INC','LLC','LP','PLC','LTD','LIMITED','COMPANY',
             'CORPORATION','INCORPORATED','INTERNATIONAL','SYSTEMS','SA','OY',
             'CONSULTING','BANK','GMBH','KABUSHIKI','KAISHA','BV','NV','SL',
             'AKTIENGESELLSCHAFT','MASCHINENFABRIK','AB','AG','AS','SPA','HF',
             'SOCIETE','ASSOCIATES','BUSINESS','INDUSTRIES','GROUP','KK',
             'LABORATORIES','WORKS','STUDIO','TELECOM','INVESTMENTS',
             'CONSULTANTS','ELECTRONICS','TECHNOLOGIES','MICROSYSTEMS',
             'MULTIMEDIA','NETWORKS','TECHNOLOGY','PARTNERSHIP','ELECTRIC',
             'COMPONENTS','AUTOMOTIVE','INSTRUMENTS','COMMUNICATION',
             'ENTERPRISES','NETWORK','ENGINEERING','DESIGNS','SCIENCES',
             'PARTNERS','AKTIENGELLSCHAFT','VENTURE','AEROSPACE',
             'PHARMACEUTICALS','DESIGN','MEDICAL','PRODUCTS','PHARMA','ENERGY',
             'SOLUTIONS','FRANCE','ISREAL','PRODUCT','PLASTICS',
             'COMMUNICATIONS','KGAA','SAS','CELLULAR','GESELLSCHAFT','SE',
             'HOLDINGS','KG','SRL','CHIMIE']
corp_re = re.compile('\\b('+'|'.join(corp_keys)+')\\b')
punc_re = re.compile(r'[0-9&()]')

nonp_keys = ['INSTITUTE','UNIVERSITY','HOSPITAL','FOUNDATION','COLLEGE',
             'RESEARCH','ADMINISTRATION','RECHERCHE','DEPARTMENT','TRUST',
             'ASSOCIATION','MINISTRY','LABORATORY','BOARD','OFFICE','UNIV',
             'ECOLE','SECRETARY','UNIVERSIDAD','SOCIETY','UNIVERSITEIT',
             'CENTRE','CENTER','NATIONAL','SCHOOL','INSTITUT','INSTITUTES',
             'UNIVERSITE']
nonp_re = re.compile('\\b('+'|'.join(nonp_keys)+')\\b')

def org_type(name):
    name = name.replace('.','')
    name = name.replace('/','')
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
other_keys = ['CHANGE','SECUR','CORRECT','RELEASE','LIEN','UPDATE','NUNC','COLLAT']
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

# connect to patent db
if store_db:
    db_fname = 'store/patents.db'
    conn = sqlite3.connect(db_fname)
    cur = conn.cursor()
    try:
        cur.execute('create table assignment (patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text, assignee_state text, assignee_country text)')
    except sqlite3.OperationalError as e:
        print(e)

# store for batch commit
batch_size = 10000
assignments = []

def commitBatch():
    cur.executemany('insert into assignment values (?,?,?,?,?,?,?,?)',assignments)
    del assignments[:]

def get_text(parent,tag,default=''):
    child = parent.find(tag)
    return (child.text or default) if child is not None else default

def patnums_gen(patents):
    for pat in patents:
        for doc in pat.findall('document-id'):
            kind = get_text(doc,'kind')
            pnum = get_text(doc,'doc-number')
            if not kind.startswith('B'):
                continue
            yield pnum

# parseahol
tcount = 0
pcount = 0
for (event,elem) in iterparse(in_fname,tag='patent-assignment',remove_blank_text=True):
    # top-level section
    record = elem.find('assignment-record')
    assignor = elem.find('patent-assignors')[0]
    assignee = elem.find('patent-assignees')[0]
    patents = elem.find('patent-properties')

    # conveyance
    convey = get_text(record,'conveyance-text')

    # names
    assignor_name = get_text(assignor,'name')
    assignee_name = get_text(assignee,'name')

    # dates
    exec_sec = assignor.find('execution-date')
    recd_sec = record.find('recorded-date')

    exec_date = get_text(exec_sec,'date') if exec_sec is not None else ''
    recd_date = get_text(recd_sec,'date') if recd_sec is not None else ''

    # location
    assignee_country = get_text(assignee,'country-name',default='UNITED STATES')
    assignee_state = get_text(assignee,'state')

    # patent info
    patnums = list(patnums_gen(patents))
    npat = len(patnums)
    if npat == 0:
        continue

    # code names
    otype = org_type(assignor_name)
    ctype = convey_type(convey)

    #if otype == ORG_INDV and ctype != CONV_OTHER and assignor_name.find(',') == -1:
    #    print('%15.15s: %40.40s -> %30.30s (%20.20s, %20.20s)' % (tcount,assignor_name,assignee_name,assignee_state,assignee_country))

    # throw out individuals
    if otype == ORG_INDV or ctype == CONV_OTHER:
        continue

    # output
    if store_db:
        for pn in patnums:
            assignments.append((pn,exec_date,recd_date,convey,assignor_name,assignee_name,assignee_state,assignee_country))
        if len(assignments) >= batch_size:
            commitBatch()

    # stats
    tcount += 1
    pcount += npat

    # break
    if tcount >= max_recs:
        break

# clear out the rest
if store_db:
    if len(assignments) > 0:
        commitBatch()

    con.commit()
    con.close()

print(tcount)
print(pcount)

