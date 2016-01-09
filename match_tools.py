# tools for name matching

# misc commands
# select firm_num,sum(revenue) as rtot from firm_merge where firm_num>=1000000 group by firm_num order by rtot desc
# select firm_num,sum(amount) as atot from firm_merge where firm_num>=2000000 group by firm_num order by atot desc

import os
import operator as op
from collections import OrderedDict
import numpy as np
import pandas as pd
import sqlite3
import pandas.io.sql as sqlio

def unfurl(v,idx=0):
  return map(op.itemgetter(idx),v)

def qset(n):
  return '('+','.join('?'*n)+')'

def argsort(seq):
  return sorted(range(len(seq)),key=seq.__getitem__)

class infobot:
  def __init__(self):
    self.con = sqlite3.connect('/Users/doug/work/patents/store/patents.db')
    self.cur = self.con.cursor()
    #self.cur.execute('attach ? as citedb',('store/citations.db',))

  def disconnect(self):
    self.con.close()

  # find all earlier matches for a specific entry
  def fnum_info(self,fnum,nshow=10):
    cur = self.cur

    # get relevant tokens
    toks = map(op.itemgetter(0),cur.execute('select tok from firm_token where firm_num=?',(fnum,)).fetchall())
    ntoks = len(toks)
    print('Looking up: ' + ' '.join(toks))
    print('')

    # clear temporary tables
    cur.execute('create table if not exists temp_match (firm_num int, ntoks int)')
    cur.execute('create table if not exists temp_sum (firm_num int, score int)')
    cur.execute('delete from temp_match')
    cur.execute('delete from temp_sum')

    # compute best match
    cur.executemany('insert into temp_match select firm_num,ntoks from firm_token where pos=? and tok=? and firm_num<1000000',enumerate(toks))
    cur.execute('insert into temp_sum select firm_num,sum(1.0/max(ntoks,?)) from temp_match group by firm_num',(ntoks,))
    fmatch = cur.execute('select firm_num,score from temp_sum order by score desc').fetchall()

    # display matches
    for (fn,score) in fmatch[:nshow]:
      print('{:8.5f}: '.format(score) + ' '.join(map(op.itemgetter(0),cur.execute('select tok from firm_token where firm_num=?',(fn,)).fetchall())))

  def search_token(self,tok,getpats=True,limit=20):
    cur = self.cur

    # get relevant firms
    fnums = unfurl(cur.execute('select firm_num from firm_token where tok=?',(tok,)).fetchall())
    fnums = fnums[:limit]
    fnames = unfurl([cur.execute('select name from firm where firm_num=?',(fn,)).fetchone() for fn in fnums])
    if getpats:
      pats = unfurl([cur.execute('select sum(file_pnum) from firmyear_info where firm_num=?',(fn,)).fetchone() for fn in fnums])
    else:
      pats = [0]*len(fnums)

    # output
    print('Looking up {}:'.format(tok))
    print('')
    print('\n'.join(['{:10d},{:10d}: {}'.format(num,pnum,name) for (num,name,pnum) in zip(fnums,fnames,pats)]))
    print('')
    print('{} matches'.format(len(fnums)))

  def firm_history(self,fnum):
    cur = self.cur

    cols = ['year','file_pnum','grant_pnum','source_pnum','dest_pnum','source_nbulk','dest_nbulk','employ','revenue','income']
    datf = pd.DataFrame(cur.execute('select '+','.join(cols)+' from firmyear_info where firm_num=?',(fnum,)).fetchall(),columns=cols)
    datf['age'] = datf['year']-datf['year'].min()
    datf['patnet'] = datf['file_pnum']+datf['dest_pnum']-datf['source_pnum']
    datf['stock'] = datf['patnet'].cumsum() - datf['patnet']
    datf['file_cum'] = datf['file_pnum'].cumsum() - datf['file_pnum']
    datf = datf.set_index('year')

    return datf

  def grants_to(self,fnum,limit=50):
    return sqlio.read_frame('select patnum,grantyear,fileyear,classone,classtwo,first_trans,ntrans,n_cited,n_citing,life_grant from grant_info where firm_num={} limit {}'.format(fnum,limit),self.con)

  def assignments_to(self,fnum):
    cur = self.cur

    ret = cur.execute("""select execyear,ntrans,firm_source.firm_num,firm_source.name from (select * from assign_bulk where dest_fn=?) as firm_assign
                         left outer join firm as firm_source on (firm_assign.source_fn = firm_source.firm_num) order by execyear""",(fnum,)).fetchall()

    return pd.DataFrame(ret,columns=['year','ntrans','source_fnum','source_name'])

  def assignments_from(self,fnum):
    cur = self.cur

    ret = cur.execute("""select execyear,ntrans,firm_dest.firm_num,firm_dest.name from (select * from assign_bulk where source_fn=?) as firm_assign
                         left outer join firm as firm_dest on (firm_assign.dest_fn = firm_dest.firm_num) order by execyear""",(fnum,)).fetchall()

    return pd.DataFrame(ret,columns=['year','ntrans','dest_fnum','dest_name'])

  def assignments_between(self,fnum_source,fnum_dest):
    cur =self.cur

    ret = cur.execute('select execyear,patnum from assign_info where source_fn=? and dest_fn=?',(fnum_source,fnum_dest)).fetchall()

    return pd.DataFrame(ret,columns=['year','patnum'])

  def word_frequency(self,tok):
    cur = self.cur

    # get relevant tokens
    count = cur.execute('select count(*) from firm_token where tok=?',(tok,)).fetchone()[0]
    mean_pos = cur.execute('select avg(pos) from firm_token where tok=?',(tok,)).fetchone()[0]

    # output
    print('Looking up {}:'.format(tok))
    print('')
    print('{:8d} instances'.format(count))
    print('{:8.5f} mean position'.format(mean_pos))

  def firm_names(self,fnums,output=False):
    cur = self.cur

    if not type(fnums) in [list,tuple]: fnums = [fnums]

    fnames = OrderedDict(zip(fnums,['']*len(fnums)))
    ret = cur.execute('select * from firm where firm_num in ('+','.join(map(str,fnums))+')').fetchall()
    for (fnum,name) in ret:
      fnames[fnum] = name
    fnames = fnames.items()

    if output:
      for (fnum,name) in fnames:
        print('{:8d}: {:s}'.format(fnum,name))

    return fnames

  def largest_by_year(self,year,num=25,col='stock'):
    cur = self.cur

    ret = cur.execute('select firm_num,file from firmyear_info where year=? and ? is not null order by ? desc limit ?',(year,col,col,num)).fetchall()

    print(ret)

  def interesting_transfers(self,min_year=0,max_year=np.inf,cite_before_min=0,cite_after_min=0,num_select=10,dest_fnum=None):
    cur = self.cur

    query = 'select patnum,source_fn,dest_fn,ncites_before,ncites_after from trans_cite_pat where ncites_before>=? and ncites_after>=? and execyear>=? and execyear<=?'
    if dest_fnum is not None: query += ' and dest_fn='+str(dest_fnum)
    ret = cur.execute(query,(cite_before_min,cite_after_min,min_year,max_year)).fetchall()
    if num_select is not None:
      ret = [ret[i] for i in np.random.randint(0,len(ret),size=num_select)]
    for (patnum,source_fn,dest_fn,ncites_before,ncites_after) in ret:
      (source_name,) = cur.execute('select name from firm where firm_num=?',(source_fn,)).fetchone()
      (dest_name,) = cur.execute('select name from firm where firm_num=?',(dest_fn,)).fetchone()
      print('{:10d} ({:3d},{:3d}): {:40.40s} -> {:40.40s}'.format(patnum,int(ncites_before),int(ncites_after),source_name,dest_name))

  def interesting_expires(self,min_year=0,max_year=np.inf,cite_before_min=0,expire_min=8,expire_max=12,num_select=10,fnum=None):
    cur = self.cur

    query = 'select patnum,firm_num,fileyear,n_citing,life_grant from grant_info where n_citing>=? and life_grant>=? and life_grant<=? and fileyear>=? and fileyear<=? limit 1000'
    ret = cur.execute(query,(cite_before_min,expire_min,expire_max,min_year,max_year)).fetchall()
    if num_select is not None:
      ret = [ret[i] for i in np.random.randint(0,len(ret),size=num_select)]
    for (patnum,firm_num,fileyear,ncites_before,life_grant) in ret:
      (firm_name,) = cur.execute('select name from firm where firm_num=?',(firm_num,)).fetchone()
      print('{:10d} ({:3d},{:3d}): {:40.40s}'.format(patnum,int(ncites_before),int(life_grant),firm_name))

  def search_owners(self,fstr):
    cur = self.cur

    ret = cur.execute('select * from owner where name like \'%' + fstr + '%\'').fetchall()

    return list(ret)

  def component_info(self,cid=None,fid=None):
    cur = self.cur

    if cid is None:
      (cid,) = cur.execute('select compid from component where ownerid=?',(fid,)).fetchone()

    names = cur.execute('select * from (select * from component where compid=?) natural join owner',(cid,)).fetchall()

    return list(names)
