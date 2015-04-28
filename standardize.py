# name standardize

import re
import collections
import itertools

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
corps = ['CORPORATION','INCORPORATED','COMPANY','LIMITED','KABUSHIKI','KAISHA','AKTIENGESELLSCHAFT','AKTIEBOLAG','INC','LLC','LTD','CORP','AG','NV','BV','GMBH','CO','BV','SA','AB','SE','KK']
dropout = states + compustat + generics + corps
gener_re = re.compile('|'.join([r"\b{}\b".format(el) for el in dropout]))

# substitutions - essentially lower their weighting
subsies = {
  'TECHNOLOGIES': 'TECH',
  'TECHNOLOGY': 'TECH',
  'MANUFACTURING': 'MANUF',
  'SEMICONDUCTORS': 'SEMI',
  'SEMICONDUCTOR': 'SEMI',
  'RESEARCH': 'RES',
  'COMMUNICATIONS': 'COMM',
  'COMMUNICATION': 'COMM',
  'SYSTEMS': 'SYS',
  'PHARMACEUTICALS': 'PHARMA',
  'PHARMACEUTICAL': 'PHARMA',
  'ELECTRONICS': 'ELEC',
  'INTERNATIONAL': 'INTL',
  'INDUSTRIES': 'INDS',
  'INDUSTRY': 'INDS',
  'CHEMICALS': 'CHEM',
  'CHEMICAL': 'CHEM'
}
subsies_re = re.compile(r'\b(' + '|'.join(subsies.keys()) + r')\b')

# standardize a firm name
def name_standardize(name):
  name_strip = name

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

  name_strip = subsies_re.sub(lambda x: subsies[x.group()],name_strip)

  return name_strip.split()

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

# match firm names for within patent data approach
fn_match_cut = 0.8
cmd_tok = 'select firm_num,ntoks from firm_token where tok=? and pos=?'
cmd_firm = 'insert into firm values (?,?)'
cmd_ftok = 'insert into firm_token values (?,?,?,?)'
def fn_match(name,cur_within,next_fn=None,output=False):
  toks = name_standardize(name)
  ntoks = len(toks)
  fn_match = collections.defaultdict(float)

  for (fn,nt) in itertools.chain(*[cur_within.execute(cmd_tok,(tok,pos)).fetchall() for (pos,tok) in enumerate(toks)]):
    fn_match[fn] += 1.0/max(nt,ntoks)

  fn_out = None
  if len(fn_match) > 0:
    best_fn = max(fn_match,key=fn_match.get)
    best_val = fn_match[best_fn]
    if best_val >= match_cut:
      fn_out = best_fn

  if next_fn:
    if fn_out == None:
      cur_within.execute(cmd_firm,(next_fn,name))
      cur_within.executemany(cmd_ftok,zip([next_fn]*ntoks,range(ntoks),toks,[ntoks]*ntoks))
      fn_out = next_fn
      next_fn += 1

    return (fn_out,next_fn)
  else:
    return fn_out

