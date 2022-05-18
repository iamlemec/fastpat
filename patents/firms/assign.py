import os
import pandas as pd

from ..tools.standardize import standardize_strong
from ..tools.tables import read_csv

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

# map to two-char state codes
state_map = {
    '': '',
    'alabama': 'al',
    'alaska': 'ak',
    'arizona': 'az',
    'arkansas': 'ar',
    'california': 'ca',
    'colorado': 'co',
    'connecticut': 'ct',
    'delaware': 'de',
    'district of columbia': 'dc',
    'florida': 'fl',
    'georgia': 'ga',
    'hawaii': 'hi',
    'idaho': 'id',
    'illinois': 'il',
    'indiana': 'in',
    'iowa': 'ia',
    'kansas': 'ks',
    'kentucky': 'ky',
    'louisiana': 'la',
    'maine': 'me',
    'maryland': 'md',
    'massachusetts': 'ma',
    'michigan': 'mi',
    'minnesota': 'mn',
    'mississippi': 'ms',
    'missouri': 'mo',
    'montana': 'mt',
    'nebraska': 'ne',
    'nevada': 'nv',
    'new hampshire': 'nh',
    'new jersey': 'nj',
    'new mexico': 'nm',
    'new york': 'ny',
    'north carolina': 'nc',
    'north dakota': 'nd',
    'ohio': 'oh',
    'oklahoma': 'ok',
    'oregon': 'or',
    'pennsylvania': 'pa',
    'rhode island': 'ri',
    'south carolina': 'sc',
    'south dakota': 'sd',
    'tennessee': 'tn',
    'texas': 'tx',
    'utah': 'ut',
    'vermont': 'vt',
    'virginia': 'va',
    'washington': 'wa',
    'west virginia': 'wv',
    'wisconsin': 'wi',
    'wyoming': 'wy'
}

# map to two-char country codes
country_map = {
    '': '',
    'united states': 'us',
    'singapore': 'sg',
    'japan': 'jp',
    'cayman islands': 'ky',
    'korea, republic of': 'kr',
    'netherlands': 'nl',
    'germany': 'de',
    'ireland': 'ie',
    'switzerland': 'ch',
    'china': 'cn',
    'france': 'fr',
    'canada': 'ca',
    'united kingdom': 'uk',
    'finland': 'fi',
    'luxembourg': 'lu',
    'taiwan': 'tw',
    'puerto rico': 'us',
    'hong kong': 'hk',
    'great britain': 'uk',
    'sweden': 'se',
    'stateless': '',
    'england': 'uk',
    'barbados': 'bb',
    'bermuda': 'bm',
    'italy': 'it',
    'denmark': 'dk',
    'israel': 'il',
    'australia': 'au',
    'ontario': 'ca',
    'belgium': 'be',
    'not provided': '',
    'virgin islands, british': 'vg',
    'cyprus': 'cy',
    'austria': 'at',
    'norway': 'no',
    'russian federation': 'ru',
    'india': 'in',
    'spain': 'es',
    'korea, democratic people\'s republic of': 'kp',
    'new zealand': 'nz',
    'guernsey': 'gg',
    'samoa': 'ws',
    'saudi arabia': 'sa',
    'mexico': 'mx',
    'malaysia': 'my',
    'brazil': 'br',
    'south africa': 'za',
    'scotland': 'uk',
    'hungary': 'hu',
    'netherlands antilles': 'an',
    'british columbia': 'ca',
    'liechtenstein': 'li',
    'seychelles': 'sc',
    'portugal': 'pt',
    'quebec': 'ca',
    'united arab emirates': 'ae',
    'german democratic republic': 'de',
    'malta': 'mt',
    'isle of man': 'im',
    'panama': 'pa',
    'bahamas': 'bs',
    'iceland': 'is',
    'georgia': 'ge',
    'channel islands': 'uk',
    'alberta': 'ca',
    'thailand': 'th',
    'czech republic': 'cz',
    'mauritius': 'mu',
    'philippines': 'ph',
    'namibia': 'na',
    'chile': 'cl',
    'saint kitts and nevis': 'kn',
    'jersey': 'je',
    'argentina': 'ar',
    'turks and caicos islands': 'tc',
    'gibraltar': 'gi',
    'manitoba': 'ca',
    'belize': 'bz',
    'wales': 'uk',
    'turkey': 'tr',
    'greece': 'gr',
    'slovakia': 'sk',
    'virgin islands, u.s.': 'us',
    'cook islands': 'ck',
    'poland': 'pl',
    'macao': 'mo',
    'nova scotia': 'ca',
    'anguilla': 'ai',
    'colombia': 'co',
    'iran, islamic republic of': 'ir',
    'monaco': 'mc',
    'marshall islands': 'mh',
    'croatia': 'hr',
    'sri lanka': 'lk',
    'venezuela': 've',
    'jordan': 'jo',
    'brunei darussalam': 'bn',
    'saskatchewan': 'ca',
    'uruguay': 'uy',
    'estonia': 'ee',
    'ukraine': 'ua',
    'northern ireland': 'uk',
    'cuba': 'cu',
    'liberia': 'lr',
    'kuwait': 'kw',
    'qatar': 'qa',
    'san marino': 'sm',
    'romania': 'ro',
    'cocos (keeling) islands': 'cc',
    'slovenia': 'si',
    'antigua and barbuda': 'ag',
    'swaziland': 'sz',
    'vanuatu': 'vu',
    'papua new guinea': 'pg',
    'latvia': 'lv',
    'union of soviet socialist republics': 'ru',
    'curacao': 'cw',
    'costa rica': 'cr',
    'viet nam': 'vn',
    'england and wales': 'uk',
    'norfolk island': 'au',
    'macau': 'mo',
    'jamaica': 'jm',
    'bulgaria': 'bg',
    'european union': 'eu',
    'trinidad and tobago': 'tt',
    'newfoundland and labrador': 'ca',
    'armenia': 'am',
    'czechoslovakia': 'cz',
    'guam': 'gu',
    'andorra': 'ad',
    'algeria': 'dz',
    'indonesia': 'id',
    'belarus': 'by',
    'azerbaijan': 'az',
    'dominica': 'dm',
    'peru': 'pe',
    'dominican republic': 'do',
    'aruba': 'aw',
    'british indian ocean territory': 'io',
    'oman': 'om',
    'guadeloupe': 'fr',
    'ecuador': 'ec',
    'lithuania': 'lt',
    'lebanon': 'lb',
    'prince edward island': 'ca',
    'kazakstan': 'kz',
    'botswana': 'bw',
    'christmas island': 'cx',
    'pakistan': 'pk',
    'american samoa': 'as',
    'saint lucia': 'lc',
    'saint vincent and the grenadines': 'vc',
    'tunisia': 'tn',
    'grenada': 'gd',
    'serbia': 'rs',
    'benin': 'bj',
    'mauritania': 'mr',
    'morocco': 'ma',
    'saint helena': 'sh',
    'bangladesh': 'bd',
    'french polynesia': 'pf',
    'chad': 'td',
    'yemen': 'ye',
    'syrian arab republic': 'sy',
    'nothern mariana islands': 'mp',
    'uzbekistan': 'uz',
    'rwanda': 'rw',
    'guatemala': 'gt',
    'new brunswick': 'ca',
    'burundi': 'bi'
}

def prune_assign(output):
    spath = os.path.join(output, 'assign_assign.csv')
    assn = read_csv(spath)

    # eliminate duplicate records - assume chronological order
    assn = assn.drop_duplicates('assignid', keep='last')
    assn = assn.dropna(subset=['patnum', 'execdate'], axis=0)

    # eliminate assignments within entities
    assn['assignee_state'] = assn['assignee_state'].map(state_map)
    assn['assignee_country'] = assn['assignee_country'].map(country_map)
    assn['same'] = assn[['assignor', 'assignee']].apply(
        lambda x: same_entity(*x), raw=True, axis=1
    )
    good = assn[~assn['same']].drop('same', axis=1)

    gpath = os.path.join(output, 'assign_use.csv')
    good.to_csv(gpath, index=False)

    # aggregated assignment stats
    pat_group = good.groupby('patnum')
    assign_stats = pd.DataFrame({
        'first_trans': pat_group['execdate'].min(),
        'n_trans': pat_group.size()
    }).reset_index()

    apath = os.path.join(output, 'assign_stats.csv')
    assign_stats.to_csv(apath, index=False)
