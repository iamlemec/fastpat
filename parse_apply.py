#!/usr/bin/env python3
# coding: UTF-8

import re
import os
import glob
from collections import defaultdict
from traceback import print_exc
from tools.parse import *
from tools.tables import ChunkWriter, DummyWriter

def parse_apply_gen2(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 2
    pat['file'] = fname

    # top-level section
    bib = elem.find('subdoc-bibliographic-information')

    # publication data
    pub = bib.find('document-id')
    if pub is not None:
        pat['pubnum'] = get_text(pub, 'doc-number')
        pat['pubdate'] = get_text(pub, 'document-date')

    # application data
    app = bib.find('domestic-filing-data')
    if app is not None:
        pat['appnum'] = get_text(app, 'application-number/doc-number')
        pat['appdate'] = get_text(app, 'filing-date')

    # title
    tech = bib.find('technical-information')
    pat['title'] = get_text(tech, 'title-of-invention')

    # ipc code
    pat['ipcs'] = []
    ipcsec = tech.find('classification-ipc')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'classification-ipc-edition').lstrip('0')
        pat['ipcs'] = [ip for ip in gen2_ipc(ipcsec)]

    # assignee name
    pat['appname'] = get_text(bib, 'assignee/organization-name')

    # first inventor address
    resid = bib.find('inventors/first-named-inventor/residence')
    if resid is not None:
        address = resid.find('residence-us')
        if address is None:
            address = resid.find('residence-non-us')
        if address is not None:
            pat['city'] = get_text(address, 'city')
            pat['state'] = get_text(address, 'state')
            pat['country'] = get_text(address, 'country-code')

    # abstract
    abst = elem.find('subdoc-abstract')
    if abst is not None:
        pat['abstract'] = raw_text(abst, sep=' ')

    # roll it in
    return pat

def parse_apply_gen3(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 3
    pat['file'] = fname

    # top-level section
    bib = elem.find('us-bibliographic-data-application')
    pubref = bib.find('publication-reference')
    appref = bib.find('application-reference')

    # published patent
    pubinfo = pubref.find('document-id')
    pat['pubnum'] = get_text(pubinfo, 'doc-number')
    pat['pubdate'] = get_text(pubinfo, 'date')

    # filing date
    pat['appnum'] = get_text(appref, 'document-id/doc-number')
    pat['appdate'] = get_text(appref, 'document-id/date')
    pat['appname'] = get_text(bib, 'assignees/assignee/addressbook/orgname')

    # title
    pat['title'] = get_text(bib, 'invention-title')

    # ipc code
    pat['ipcs'] = []
    ipcsec = bib.find('classification-ipc')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'edition').lstrip('0')
        pat['ipcs'] = [ip for ip in gen3a_ipc(ipcsec)]
    else:
        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            pat['ipcver'] = get_text(ipcsec, 'classification-ipcr/ipc-version-indicator/date')
            pat['ipcs'] = [ip for ip in gen3r_ipc(ipcsec)]

    # first inventor address
    address = bib.find('parties/applicants/applicant/addressbook/address')
    if address is None:
        address = bib.find('us-parties/us-applicants/us-applicant/addressbook/address')
    if address is not None:
        pat['city'] = get_text(address, 'city')
        pat['state'] = get_text(address, 'state')
        pat['country'] = get_text(address, 'country')

    # abstract
    abspar = elem.find('abstract')
    if abspar is not None:
        pat['abstract'] = raw_text(abspar, sep=' ')

    # roll it in
    return pat

# table schema
schema_apply = {
    'appnum': 'str', # Patent number
    'appdate': 'str', # Application date
    'appname': 'str', # Assignee name
    'pubnum': 'str', # Publication number
    'pubdate': 'str', # Publication date
    'ipc': 'str', # Main IPC code
    'ipcver': 'str', # IPC version info
    'city': 'str', # Assignee city
    'state': 'str', # State code
    'country': 'str', # Assignee country
    'title': 'str', # Title
    'abstract': 'str', # Abstract
    'gen': 'int', # USPTO data format
    'file': 'str', # path to source file
}

schema_ipc = {
    'patnum': 'str', # Patent number
    'ipc': 'str', # IPC code
    'rank': 'int', # Order listed
    'version': 'str' # IPC version
}

# chunking express
def store_patent(pat, chunker_pat, chunker_ipc):
    an, iv = pat['appnum'], pat['ipcver']

    # store ipcs
    for j, ipc in enumerate(pat['ipcs']):
        if j == 0: pat['ipc'] = ipc
        chunker_ipc.insert(an, ipc, j, iv)

    # store patent
    chunker_pat.insert(*(pat.get(k, '') for k in schema_apply))

# file level
def parse_file(fpath, output, overwrite=False, dryrun=False, display=0):
    fdir, fname = os.path.split(fpath)
    ftag, fext = os.path.splitext(fname)

    opath = os.path.join(output, ftag)
    opath_apply = f'{opath}_apply.csv'
    opath_ipc = f'{opath}_ipc.csv'

    if not overwrite:
        if os.path.exists(opath_apply) and os.path.exists(opath_ipc):
            print(f'{ftag}: Skipping')
            return

    if fname.startswith('pab'):
        gen = 2
        main_tag = 'patent-application-publication'
        parser = lambda fp: parse_wrapper(fp, 'patent-application-publication', parse_apply_gen2)
    elif fname.startswith('ipab'):
        gen = 3
        parser = lambda fp: parse_wrapper(fp, 'us-patent-application', parse_apply_gen3)
    else:
        raise Exception(f'{ftag}: Unknown format')

    if not dryrun:
        chunker_apply = ChunkWriter(opath_apply, schema=schema_apply)
        chunker_ipc = ChunkWriter(opath_ipc, schema=schema_ipc)
    else:
        chunker_apply = DummyWriter()
        chunker_ipc = DummyWriter()

    # parse it up
    try:
        print(f'{ftag}: Starting')

        i = 0
        for pat in parser(fpath):
            i += 1

            store_patent(pat, chunker_apply, chunker_ipc)

            # output
            if display > 0 and i % display == 0:
                spat = {k: pat.get(k, '') for k in schema_apply}
                print('an = {appnum:10.10s}, fd = {appdate:10.10s}, ti = {title:30.30s}, on = {appname:30.30s}, ci = {city:15.15s}, st = {state:2s}, ct = {country:2s}'.format(**spat))

        # commit to db and close
        chunker_apply.commit()
        chunker_ipc.commit()

        print(f'{ftag}: Parsed {i} patents')
    except Exception as e:
        print(f'{ftag}: EXCEPTION OCCURRED!')
        print_exc()

        chunker_apply.delete()
        chunker_ipc.delete()

if __name__ == '__main__':
    import argparse
    from multiprocessing import Pool

    # parse input arguments
    parser = argparse.ArgumentParser(description='patent application parser')
    parser.add_argument('target', type=str, nargs='*', help='path or directory of file(s) to parse')
    parser.add_argument('--output', type=str, default='parsed/apply', help='directory to output to')
    parser.add_argument('--display', type=int, default=1000, help='how often to display summary')
    parser.add_argument('--dryrun', action='store_true', help='do not actually store')
    parser.add_argument('--overwrite', action='store_true', help='clobber existing files')
    parser.add_argument('--cores', type=int, default=10, help='number of cores to use')
    args = parser.parse_args()

    # collect files
    if len(args.target) == 0 or (len(args.target) == 1 and os.path.isdir(args.target[0])):
        targ_dir = 'data/apply' if len(args.target) == 0 else args.target[0]
        file_list = sorted(glob.glob(f'{targ_dir}/pab*.xml')) + sorted(glob.glob(f'{targ_dir}/ipab*.xml'))
    else:
        file_list = args.target

    # ensure output dir
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    # apply options
    opts = dict(overwrite=args.overwrite, dryrun=args.dryrun, display=args.display)
    def parse_file_opts(fpath):
        parse_file(fpath, args.output, **opts)

    # parse files
    with Pool(args.cores) as pool:
        pool.map(parse_file_opts, file_list, chunksize=1)
