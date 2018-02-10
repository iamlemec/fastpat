import re
import os
import sys
import glob
import sqlite3
import argparse
from lxml.etree import iterparse
from parse_common import clear, get_text, raw_text, ChunkInserter
from traceback import print_exc

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

# MAIN SECTION

# parse input arguments
parser = argparse.ArgumentParser(description='USPTO patent assignment parser.')
parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
parser.add_argument('--db', type=str, default=None, help='database file to store to')
parser.add_argument('--limit', type=int, default=None, help='only parse n patents')
args = parser.parse_args()

# connect to patent db
con = sqlite3.connect(args.db)
cur = con.cursor()
cur.execute('create table if not exists assign (assignid integer primary key, patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text, assignee_state text, assignee_country text)')
cur.execute('create unique index if not exists idx_assign on assign (patnum,execdate,assignor,assignee)')
chunker = ChunkInserter(con, table='assign')

def gen_patnums(patents):
    for pat in patents:
        for doc in pat.findall('document-id'):
            kind = get_text(doc, 'kind')
            pnum = get_text(doc, 'doc-number')
            if not kind.startswith('B'):
                continue
            yield pnum

# parseahol
i = 0
o = 0
p = 0
def parse_gen3(fname_in):
    global i, o, p

    for (event,elem) in iterparse(fname_in, tag='patent-assignment', events=['end'], recover=True):
        # top-level section
        record = elem.find('assignment-record')
        assignor = elem.find('patent-assignors')[0]
        assignee = elem.find('patent-assignees')[0]
        patents = elem.find('patent-properties')

        # conveyance
        convey = get_text(record,'conveyance-text')

        # names
        assignor_name = get_text(assignor, 'name')
        assignee_name = get_text(assignee, 'name')

        # dates
        exec_sec = assignor.find('execution-date')
        recd_sec = record.find('recorded-date')

        exec_date = get_text(exec_sec, 'date') if exec_sec is not None else ''
        recd_date = get_text(recd_sec, 'date') if recd_sec is not None else ''

        # location
        assignee_country = get_text(assignee, 'country-name', default='UNITED STATES')
        assignee_state = get_text(assignee, 'state')

        # patent info
        patnums = list(gen_patnums(patents))
        npat = len(patnums)
        if npat == 0:
            continue

        # code names
        src_type = org_type(assignor_name)
        dst_type = org_type(assignee_name)
        ctype = convey_type(convey)

        # throw out individuals
        if src_type == ORG_INDV or dst_type == ORG_INDV or ctype == CONV_OTHER:
            o += 1
            continue

        # output
        for pn in patnums:
            chunker.insert(None, pn, exec_date, recd_date, convey, assignor_name, assignee_name, assignee_state, assignee_country)

        # free memory
        clear(elem)

        # stats
        i += 1
        p += npat

        # logging
        if i % 1000 == 0:
            print('%4d: %40.40s -> %30.30s (%20.20s, %20.20s)' % (npat, assignor_name, assignee_name, assignee_state, assignee_country))

        # break
        if args.limit and i >= args.limit:
            return False

    return True

# collect files
if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
    targ_dir = 'assign_files' if len(args.target) == 0 else args.target[0]
    file_list = sorted(glob.glob('%s/*.xml' % targ_dir))
else:
    file_list = args.target

# do robust parsing
for fpath in file_list:

    # terminate on limit
    if args.limit is not None and i >= args.limit:
        print('Reached limit.')
        break

    (fdir, fname) = os.path.split(fpath)
    print('Parsing %s' % fname)
    i0, o0, p0 = i, o, p

    try:
        parse_gen3(fpath)
    except Exception as e:
        print('EXCEPTION OCCURRED!')
        print_exc()

    print('Found %d records, %d dropped, %d patents' % (i-i0, o-o0, p-p0))
    print('Total %d records, %d dropped, %d patents' % (i, o, p))
    print()

# clear out the rest
chunker.commit()
con.close()
