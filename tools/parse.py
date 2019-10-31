##
## common parsing tools
##

import os
import re
import numpy as np
import pandas as pd
from lxml.etree import XMLPullParser

##
## xml parsing
##

# get descendent text
def get_text(parent, tag, default=''):
    child = parent.find(tag)
    if child is None:
        return default
    elif child.text is None:
        return default
    else:
        return child.text.strip().lower()

# get all text of node
def raw_text(par, sep=''):
    return sep.join(par.itertext()).strip().lower()

# preserve memory
def clear(elem):
    elem.clear()
    while elem.getprevious() is not None:
        del elem.getparent()[0]

# parse mangled xml
def parse_wrapper(fpath, main_tag, parser):
    _, fname = os.path.split(fpath)
    pp = XMLPullParser(tag=main_tag, events=['end'], recover=True)
    def parse_all():
        for _, pat in pp.read_events():
            yield parser(pat, fname)
            clear(pat)

    with open(fpath, errors='ignore') as f:
        pp.feed('<root>\n')
        for line in f:
            if line.startswith('<?xml'):
                yield from parse_all()
            elif line.startswith('<!DOCTYPE') or line.startswith('<!ENTITY') or line.startswith(']>'):
                pass
            else:
                pp.feed(line)
        else:
            pp.feed('</root>\n')
            yield from parse_all()

##
## patnum parsers
##

# standard way to pruce patent numbers (allows for all types)
def prune_patnum(pn, maxlen=7):
    ret = re.match(r'([a-zA-Z]{1,2}|0)?([0-9]+)', pn)
    if ret is None:
        prefix = ''
        patnum = pn
    else:
        prefix, patnum = ret.groups()
        prefix = '' if (prefix is None or prefix == '0') else prefix
    patnum = patnum[:maxlen].lstrip('0')
    return prefix + patnum

##
## ipc parsers
##

# early grant only (1, 1.5)
def pad_ipc(ipc):
    if len(ipc) >= 8:
        return ipc[:4] + ipc[4:7].replace(' ', '0') + '/' + ipc[7:]
    else:
        return ipc

# grant only (1.5)
def gen15_ipc(ipcsec):
    yield get_text(ipcsec, 'B511/PDAT')
    for ipc in ipcsec.findall('B512'):
        yield get_text(ipc, 'PDAT')

# apply only (2)
def gen2_ipc(ipcsec):
    yield get_text(ipcsec, 'classification-ipc-primary/ipc')
    for ipc in ipcsec.findall('classification-ipc-secondary'):
        yield get_text(ipc, 'ipc')

# apply and grant (3)
def gen3a_ipc(ipcsec):
    yield get_text(ipcsec, 'main-classification')
    for ipc in ipcsec.findall('further-classification'):
        yield ipc.text or ''

def gen3g_ipc(ipcsec):
    yield get_text(ipcsec, 'main-classification')
    for ipc in ipcsec.findall('further-classification'):
        yield ipc.text or ''

# apply and grant (3)
def gen3r_ipc(ipcsec):
    for ipc in ipcsec.findall('classification-ipcr'):
        yield get_text(ipc, 'section') + get_text(ipc, 'class') + get_text(ipc, 'subclass') \
            + get_text(ipc, 'main-group').zfill(3) + '/' + get_text(ipc, 'subgroup')

##
## cite parsers
##

# grant (2)
def gen2_cite(refs):
    for cite in refs.findall('B561'):
        yield get_text(cite, 'PCIT/DOC/DNUM/PDAT')

# grant (3)
def gen3_cite(refs, prefix):
    for cite in refs.findall(prefix+'citation/patcit/document-id'):
        natl = get_text(cite, 'country')
        kind = get_text(cite, 'kind')
        pnum = get_text(cite, 'doc-number')
        if natl == 'us' and kind != '00': # US granted patents only
            yield pnum

##
## assign parsers
##

def gen3_assign(patents):
    for doc in patents.findall('patent-property/document-id'):
        kind = get_text(doc, 'kind')
        pnum = get_text(doc, 'doc-number')
        if kind.startswith('b'):
            yield pnum

# detect organization type
ORG_CORP = 0
ORG_NONP = 1
ORG_INDV = 2

LEN_CUT = 30

corp_keys = ['corp', 'co', 'inc', 'llc', 'lp', 'plc', 'ltd', 'limited', 'company', 'corporation', 'incorporated', 'international', 'systems', 'sa', 'oy', 'consulting', 'bank', 'gmbh', 'kabushiki', 'kaisha', 'bv', 'nv', 'sl', 'aktiengesellschaft', 'maschinenfabrik', 'ab', 'ag', 'as', 'spa', 'hf', 'societe', 'associates', 'business', 'industries', 'group', 'kk', 'laboratories', 'works', 'studio', 'telecom', 'investments', 'consultants', 'electronics', 'technologies', 'microsystems', 'multimedia', 'networks', 'technology', 'partnership', 'electric', 'components', 'automotive', 'instruments', 'communication', 'enterprises', 'network', 'engineering', 'designs', 'sciences', 'partners', 'aktiengellschaft', 'venture', 'aerospace', 'pharmaceuticals', 'design', 'medical', 'products', 'pharma', 'energy', 'solutions', 'france', 'isreal', 'product', 'plastics', 'communications', 'kgaa', 'sas', 'cellular', 'gesellschaft', 'se', 'holdings', 'kg', 'srl', 'chimie']
nonp_keys = ['institute', 'university', 'hospital', 'foundation', 'college', 'research', 'administration', 'recherche', 'department', 'trust', 'association', 'ministry', 'laboratory', 'board', 'office', 'univ', 'ecole', 'secretary', 'universidad', 'society', 'universiteit', 'centre', 'center', 'national', 'school', 'institut', 'institutes', 'universite']

punc_re = re.compile(r'[0-9&()]')
spac_re = re.compile(r'[ ,]')
corp_re = re.compile('\\b('+'|'.join(corp_keys)+')\\b')
nonp_re = re.compile('\\b('+'|'.join(nonp_keys)+')\\b')

def org_type(name):
    name = name.replace('.', '')
    name = name.replace('/', '')
    has_corp = corp_re.search(name) != None
    has_nonp = nonp_re.search(name) != None
    has_punc = punc_re.search(name) != None
    has_spac = spac_re.search(name) != None
    long_name = len(name) > LEN_CUT
    if has_corp or has_punc or not has_spac or long_name:
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
other_keys = ['change', 'secur', 'correct', 'release', 'lien', 'update', 'nunc', 'collat']
other_re = re.compile('|'.join(other_keys))

def convey_type(convey):
    if other_re.search(convey) != None:
        return CONV_OTHER
    elif convey.find('assign') != -1:
        return CONV_ASSIGN
    elif convey.find('license') != -1:
        return CONV_LICENSE
    elif convey.find('merge') != -1:
        return CONV_MERGER
    else:
        return CONV_OTHER
