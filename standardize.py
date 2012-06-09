# name standardize

import re
import collections

# postscripts
post0 = r"(\bA CORP.|;|,).*$"
post0_re = re.compile(post0)

# acronyms
acronym1 = r"\b(\w) (\w) (\w)\b"
acronym1_re = re.compile(acronym1)
acronym2 = r"\b(\w) (\w)\b"
acronym2_re = re.compile(acronym2)
acronym3 = r"\b(\w)-(\w)-(\w)\b"
acronym3_re = re.compile(acronym3)
acronym4 = r"\b(\w)-(\w)\b"
acronym4_re = re.compile(acronym4)
acronym5 = r"\b(\w\w)&(\w)\b"
acronym5_re = re.compile(acronym5)
acronym6 = r"\b(\w)&(\w)\b"
acronym6_re = re.compile(acronym6)
acronym7 = r"\b(\w) & (\w)\b"
acronym7_re = re.compile(acronym7)

# punctuation
punct0 = r"'S|\(.*\)|\."
punct1 = r"[^\w\s]"
punct0_re = re.compile(punct0)
punct1_re = re.compile(punct1)

# generic terms
states = ['DEL','DE','NY','VA','CA','PA','OH','NC','WI','MA']
compustat = ['PLC','CL','REDH','ADR','FD','LP','CP','TR','SP','COS','GP','OLD','NEW']
generics = ['THE','A','OF','AND','AN']
corps = ['CORPORATION','INCORPORATED','COMPANY','LIMITED','KABUSHIKI','KAISHA','AKTIENGESELLSCHAFT','AKTIEBOLAG','INC','LLC','LTD','CORP','AG','NV','BV','GMBH','CO','BV','SA','AB','SE']
typos = ['CORPORATIN','CORPORATON']
variants = ['TRUST','GROUP','GRP','HLDGS','HOLDINGS','COMM','INDS','COMM','HLDG','TECH','INTERNATIONAL']
dropout = states + compustat + generics + corps + typos + variants
gener_re = re.compile('|'.join([r"\b{}\b".format(el) for el in dropout]))

# standardize a firm name
def name_standardize(name):
  name_strip = name
  name_strip = post0_re.sub('',name_strip)
  name_strip = acronym1_re.sub(r"\1\2\3",name_strip)
  name_strip = acronym2_re.sub(r"\1\2",name_strip)
  name_strip = acronym3_re.sub(r"\1\2\3",name_strip)
  name_strip = acronym4_re.sub(r"\1\2",name_strip)
  name_strip = acronym5_re.sub(r"\1\2",name_strip)
  name_strip = acronym6_re.sub(r"\1\2",name_strip)
  name_strip = acronym7_re.sub(r"\1\2",name_strip)
  name_strip = punct0_re.sub('',name_strip)
  name_strip = punct1_re.sub(' ',name_strip)
  name_strip = gener_re.sub('',name_strip)
  name_toks = name_strip.split()
  return name_toks


# detect matches
cmd_name = 'select name from firmname where gvkey=?'
cmd_key = 'select gvkey,idx,ntoks from firmkey where keyword=?'
cmd_key_wgt = 'select gvkey,idx,ntoks,weight,wgt_tot from firmkey where keyword=?'

# unweighted
match_cut = 1.0
def detect_match(name,cur_comp,output=False):
  gv_match = collections.defaultdict(float)

  keys = name_standardize(name)
  nkeys = len(keys)

  for (key,idx) in zip(keys,range(nkeys)):
    for (gvkey,pos,ntoks) in cur_comp.execute(cmd_key,(key,)):
      if idx == pos:
        gv_match[gvkey] += 1.0/max(nkeys,ntoks)

  gv_out = None
  best_val = 0.0
  if len(gv_match) > 0:
    best_gv = max(gv_match,key=gv_match.get)
    best_val = gv_match[best_gv]
    if best_val >= match_cut:
      gv_out = best_gv

  if output:
    if best_val >= 0.50:
      (match_name,) = cur_comp.execute(cmd_name,(best_gv,)).fetchall()[0]
      match_txt = 'MATCH' if (gv_out != None) else 'NONE'
      print '({:5.5}, {:10.5}): {:30.30} -> {:30.30} ({:10})'.format(match_txt,best_val,name,match_name,best_gv)

  return gv_out

# weighted
match_cut_wgt = 0.97
def detect_match_wgt(name,cur_comp,output=False):
  gv_match = collections.defaultdict(float)
  gv_weight = {}

  keys = name_standardize(name)
  nkeys = len(keys)
  wgt_dict = {}

  for (key,idx) in zip(keys,range(nkeys)):
    wgt_dict[key] = 1.0 # default value if we don't find anything
    for (gvkey,pos,ntoks,weight,gv_wgt_tot) in cur_comp.execute(cmd_key_wgt,(key,)):
      wgt_dict[key] = weight
      gv_weight[gvkey] = gv_wgt_tot
      if idx == pos:
        gv_match[gvkey] += weight

  wgt_tot = sum(wgt_dict.values())
  for gvkey in gv_match:
    gv_match[gvkey] /= max(wgt_tot,gv_weight[gvkey])

  gv_out = None
  best_gv = None
  best_val = 0.0
  if len(gv_match) > 0:
    best_gv = max(gv_match,key=gv_match.get)
    best_val = gv_match[best_gv]
    if best_val >= match_cut_wgt:
      gv_out = best_gv

  if output:
    if best_val >= 0.90:
      (match_name,) = cur_comp.execute(cmd_name,(best_gv,)).fetchall()[0]
      match_txt = 'MATCH' if (gv_out != None) else 'NONE'
      print '({:5.5}, {:10.5}): {:30.30} -> {:30.30} ({:10})'.format(match_txt,best_val,name,match_name,best_gv)

  return gv_out


