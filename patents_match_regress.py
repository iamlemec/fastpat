import pandas.stats.api as ps
import statsmodels.api as sm

# regression number
if len(sys.argv) == 1:
  reg_num = 0
else:
  reg_num = int(sys.argv[1])

# industry dummy variables - this includes all industries, so subs in for intercept
datf_base = datf_rnd
sel_year = (datf_base['year']>=1985)&(datf_base['year']<=2005)
datf_use = datf_base[sel_year]
N = len(datf_use)

naics2_names = map(lambda i: 'naics_{:02d}'.format(i),naics2_set)
naics2_dummies = np.vstack([datf_use['naics2'].values==n for n in naics2_set]).astype(np.float)
naics2_dict = dict(zip(naics2_names,naics2_dummies))

naics3_names = map(lambda i: 'naics_{:02d}'.format(i),naics3_set)
naics3_dummies = np.vstack([datf_use['naics3'].values==n for n in naics3_set]).astype(np.float)
naics3_dict = dict(zip(naics3_names,naics3_dummies))

# exogenous variables
year = datf_use['year']-datf_use['year'].mean()
age = datf_use['age']-datf_use['age'].mean()
l_rnd = np.log(datf_use['rnd'])
l_revn = np.log(datf_use['revenue'])
l_stock = np.log(datf_use['stock'])
file1 = datf_use['file_next']

# standard R&D regression on comp/rnd firms with industry effects
if reg_num == 1:
  dat = pandas.DataFrame(dict({'file':file1,'l_rnd':l_rnd,'l_stock':l_stock,'year':year,'age':age},**naics3_dict)).dropna()
  y = dat['file']
  x = dat.filter(['l_rnd','l_stock','year','age']+naics3_names)

  model = sm.GLM(y,x,family=sm.families.Poisson())
  results = model.fit()
  print results.summary()

# age-size-year regression on source/dest/file
if reg_num == 2:
  y_var = 'file_next'
  y_vec = datf_idx[y_var]
  dat = pandas.DataFrame({y_var:y_vec,'year':year,'age':age,'l_stock':l_stock}).dropna()
  dat['intercept'] = 1.0

  y = dat[y_var]
  x = dat.filter(['intercept','year','age','l_stock'])

  model = sm.GLM(y,x,family=sm.families.Poisson())
  results = model.fit()
  print results.summary()

# age-size-year-industry(2) regression on source/dest/file
if reg_num == 3:
  y_var = 'dest_next'
  y_vec = datf_use[y_var]
  dat = pandas.DataFrame(dict({y_var:y_vec,'year':year,'age':age,'l_stock':l_stock},**naics2_dict)).dropna()

  y = dat[y_var]
  x = dat.filter(['year','age','l_stock']+naics2_names)

  model = sm.GLM(y,x,family=sm.families.Poisson())
  results = model.fit()
  print results.summary()

# age-size-year-industry(3) regression on source/dest/file
if reg_num == 4:
  y_var = 'dest_next'
  y_vec = datf_use[y_var]
  dat = pandas.DataFrame(dict({y_var:y_vec,'year':year,'age':age,'l_stock':l_stock},**naics3_dict)).dropna()

  y = dat[y_var]
  x = dat.filter(['year','age','l_stock']+naics3_names)

  model = sm.GLM(y,x,family=sm.families.Poisson())
  results = model.fit()
  print results.summary()

