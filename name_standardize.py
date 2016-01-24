import re

#
# weak name standardization
#

# regular expression substitutions
paren = r"'S|\(.*\)|\."
punct = r"[^\w\s]"
space = r"[ ]{2,}"

paren_re = re.compile(paren)
punct_re = re.compile(punct)
space_re = re.compile(space)

# standardize firm name
def name_standardize_weak(name):
    name_strip = name
    name_strip = paren_re.sub(' ',name_strip)
    name_strip = punct_re.sub(' ',name_strip)
    name_strip = space_re.sub(' ',name_strip)
    name_strip = name_strip.strip()
    return name_strip

#
# strong name standardization
#

# whitespace
white0 = r" +"
white0_re = re.compile(white0)

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
en_corps = ['CORPORATION','INCORPORATED','COMPANY','LIMITED','INC','LLC','LTD','CORP']
eu_corps = ['AKTIENGESELLSCHAFT','AKTIEBOLAG','AG','NV','BV','GMBH','CO','BV','SA','AB','SE']
jp_corps = ['KABUSHIKI','KAISHA','SEISAKUSHO']
variants = ['TRUST','GROUP','GRP','HLDGS','HOLDINGS','COMM','INDS','COMM','HLDG','TECH','INTERNATIONAL','CORP','COMP']
dropout = states + compustat + generics + en_corps + eu_corps + jp_corps + variants
gener0_re = re.compile('|'.join([r"\b{}\b".format(el) for el in dropout]))

# standardize a firm name
def name_standardize_strong(name):
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
    name_strip = gener0_re.sub('',name_strip)
    name_strip = white0_re.sub(' ',name_strip)
    return name_strip.strip()
