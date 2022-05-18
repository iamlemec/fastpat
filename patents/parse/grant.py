import re
import os
import glob
from collections import defaultdict
from traceback import print_exc
from itertools import chain
from multiprocessing import Pool

from ..tools.parse import *
from ..tools.tables import ChunkWriter, DummyWriter

# parse it up
def parse_grant_gen1(fname):
    pat = None
    sec = None
    tag = None
    ipcver = None
    for nline in chain(open(fname, encoding='latin1', errors='ignore'), ['PATN']):
        # peek at next line
        ntag, nbuf = nline[:4].rstrip(), nline[5:-1].rstrip().lower()
        if tag is None:
            tag = ntag
            buf = nbuf
            continue
        if ntag == '':
            buf += nbuf
            continue

        # regular tags
        if tag == 'PATN':
            if pat is not None:
                pat['appnum'] = src + apn
                yield pat
            pat = defaultdict(str)
            sec = 'PATN'
            pat['gen'] = 1
            _, pat['file'] = os.path.split(fname)
            pat['ipcs'] = []
            pat['cites'] = []
            src, apn = '', ''
        elif tag in ['INVT', 'ASSG', 'PRIR', 'CLAS', 'UREF', 'FREF', 'OREF', 'LREP', 'PCTA', 'ABST']:
            sec = tag
        elif tag in ['PAL', 'PAR', 'PAC', 'PA0', 'PA1']:
            if sec == 'ABST':
                if 'abstract' not in pat:
                    pat['abstract'] = buf
                else:
                    pat['abstract'] += '\n' + buf
        elif tag == 'WKU':
            if sec == 'PATN':
                pat['patnum'] = prune_patnum(buf)
        elif tag == 'SRC':
            if sec == 'PATN':
                src = '29' if buf == 'd' else buf.zfill(2) # design patents get series code 29
        elif tag == 'APN':
            if sec == 'PATN':
                apn = buf[:6]
        elif tag == 'ISD':
            if sec == 'PATN':
                pat['pubdate'] = buf
        elif tag == 'APD':
            if sec == 'PATN':
                pat['appdate'] = buf
        elif tag == 'ICL':
            if sec == 'CLAS':
                pat['ipcs'].append(pad_ipc(buf))
        elif tag == 'EDF':
            if sec == 'CLAS':
                pat['ipcver'] = buf
        elif tag == 'TTL':
            if sec == 'PATN':
                pat['title'] = buf
        elif tag == 'NCL':
            if sec == 'PATN':
                pat['claims'] = buf
        elif tag == 'NAM':
            if sec == 'ASSG':
                pat['owner'] = buf
        elif tag == 'CTY':
            if sec == 'ASSG':
                pat['city'] = buf
        elif tag == 'STA':
            if sec == 'ASSG':
                pat['state'] = buf
                pat['country'] = 'us'
        elif tag == 'CNT':
            if sec == 'ASSG':
                pat['country'] = buf[:2]
        elif tag == 'PNO':
            if sec == 'UREF':
                pat['cites'].append(prune_patnum(buf))

        # stage next tag and buf
        tag = ntag
        buf = nbuf

def parse_grant_gen2(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 2
    pat['file'] = fname

    # top-level section
    bib = elem.find('SDOBI')

    # published patent
    pubref = bib.find('B100')
    pat['patnum'] = prune_patnum(get_text(pubref, 'B110/DNUM/PDAT'))
    pat['pubdate'] = get_text(pubref, 'B140/DATE/PDAT')

    # filing date
    appref = bib.find('B200')
    pat['appnum'] = get_text(appref, 'B210/DNUM/PDAT')
    pat['appdate'] = get_text(appref, 'B220/DATE/PDAT')

    # reference info
    patref = bib.find('B500')
    ipcsec = patref.find('B510')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'B516/PDAT')
        pat['ipcs'] = [pad_ipc(ip) for ip in gen15_ipc(ipcsec)]
    else:
        pat['ipcs'] = []
    pat['title'] = get_text(patref, 'B540/STEXT/PDAT')
    pat['claims'] = get_text(patref, 'B570/B577/PDAT')

    # citations
    refs = patref.find('B560')
    if refs is not None:
        pat['cites'] = [prune_patnum(pn) for pn in gen2_cite(refs)]
    else:
        pat['cites'] = []

    # applicant name and address
    ownref = bib.find('B700/B730/B731/PARTY-US')
    if ownref is not None:
        pat['owner'] = get_text(ownref, 'NAM/ONM/STEXT/PDAT')
        address = ownref.find('ADR')
        if address is not None:
            pat['city'] = get_text(address, 'CITY/PDAT')
            pat['state'] = get_text(address, 'STATE/PDAT')
            pat['country'] = get_text(address, 'CTRY/PDAT')

    # abstract
    abspars = elem.findall('SDOAB/BTEXT/PARA')
    if len(abspars) > 0:
        pat['abstract'] = '\n'.join([raw_text(e) for e in abspars])

    # roll it in
    return pat

def parse_grant_gen3(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 3
    pat['file'] = fname

    # top-level section
    bib = elem.find('us-bibliographic-data-grant')
    pubref = bib.find('publication-reference')
    appref = bib.find('application-reference')

    # published patent
    pubinfo = pubref.find('document-id')
    pat['patnum'] = prune_patnum(get_text(pubinfo, 'doc-number'), maxlen=8)
    pat['pubdate'] = get_text(pubinfo, 'date')

    # filing date
    appinfo = appref.find('document-id')
    pat['appnum'] = get_text(appinfo, 'doc-number')
    pat['appdate'] = get_text(appinfo, 'date')

    # title
    pat['title'] = get_text(bib, 'invention-title')

    # ipc code
    pat['ipcs'] = []
    ipcsec = bib.find('classification-ipc')
    if ipcsec is not None:
        pat['ipcver'] = get_text(ipcsec, 'edition')
        pat['ipcs'] = [pad_ipc(ip) for ip in gen3g_ipc(ipcsec)]
    else:
        ipcsec = bib.find('classifications-ipcr')
        if ipcsec is not None:
            pat['ipcver'] = get_text(ipcsec, 'classification-ipcr/ipc-version-indicator/date')
            pat['ipcs'] = [ip for ip in gen3r_ipc(ipcsec)]

    # claims
    pat['claims'] = get_text(bib, 'number-of-claims')

    # citations
    refs = bib.find('references-cited')
    prefix = ''
    if refs is None:
        refs = bib.find('us-references-cited')
        prefix = 'us-'
    if refs is not None:
        pat['cites'] = [prune_patnum(pn, maxlen=8) for pn in gen3_cite(refs, prefix)]
    else:
        pat['cites'] = []

    # applicant name and address
    assignee = bib.find('assignees/assignee/addressbook')
    if assignee is not None:
        pat['owner'] = get_text(assignee, 'orgname')
        address = assignee.find('address')
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

# table schemas
schema_grant = {
    'patnum': 'str', # Patent number
    'pubdate': 'str', # Publication date
    'appnum': 'str', # Application number
    'appdate': 'str', # Publication date
    'ipc': 'str', # Main IPC code
    'ipcver': 'str', # IPC version info
    'city': 'str', # Assignee city
    'state': 'str', # State code
    'country': 'str', # Assignee country
    'owner': 'str', # Assignee name
    'claims': 'int', # Independent claim
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

schema_cite = {
    'src': 'str', # Source patent (citer)
    'dst': 'str' # Destination patent (citee)
}

# patent adder
def store_patent(pat, chunker_grant, chunker_ipc, chunker_cite):
    pn, iv = pat['patnum'], pat['ipcver']

    # store cites
    for cite in pat['cites']:
        chunker_cite.insert(pn, cite)

    # store ipcs
    for j, ipc in enumerate(pat['ipcs']):
        if j == 0:
            pat['ipc'] = ipc
        chunker_ipc.insert(pn, ipc, j, iv)

    # store patent
    chunker_grant.insert(*(pat.get(k, '') for k in schema_grant))

# file level
def parse_file(fpath, output, display=0, overwrite=False, dryrun=False):
    fdir, fname = os.path.split(fpath)
    ftag, fext = os.path.splitext(fname)

    opath_grant = os.path.join(output, f'grant_{ftag}.csv')
    opath_ipc = os.path.join(output, f'ipc_{ftag}.csv')
    opath_cite = os.path.join(output, f'cite_{ftag}.csv')

    complete = (
        os.path.exists(opath_grant) and
        os.path.exists(opath_ipc) and
        os.path.exists(opath_cite)
    )
    if not overwrite and complete:
        print(f'{ftag}: Skipping')
        return

    if dryrun:
        chunker_grant = DummyWriter()
        chunker_ipc = DummyWriter()
        chunker_cite = DummyWriter()
    else:
        chunker_grant = ChunkWriter(opath_grant, schema=schema_grant)
        chunker_ipc = ChunkWriter(opath_ipc, schema=schema_ipc)
        chunker_cite = ChunkWriter(opath_cite, schema=schema_cite)

    if fname.endswith('.dat'):
        gen = 1
        parser = parse_grant_gen1
    elif fname.startswith('pgb'):
        gen = 2
        parser = lambda fp: parse_wrapper(fp, 'PATDOC', parse_grant_gen2)
    elif fname.startswith('ipgb'):
        gen = 3
        parser = lambda fp: parse_wrapper(fp, 'us-patent-grant', parse_grant_gen3)
    else:
        print(f'{ftag}: Unknown format')

    # parse it up
    try:
        print(f'{ftag}: Starting')

        i = 0
        for pat in parser(fpath):
            i += 1

            # store all info
            store_patent(pat, chunker_grant, chunker_ipc, chunker_cite)

            # output if needed
            if display > 0 and i % display == 0:
                spat = {k: pat.get(k, '') for k in schema_grant}
                print(
                    'pn = {patnum:10.10s}, pd = {pubdate:10.10s}, ti = {title:30.30s}, '
                    'on = {owner:30.30s}, ci = {city:15.15s}, st = {state:2s}, '
                    'ct = {country:2s}'.format(**spat)
                )

        # commit remaining
        chunker_grant.commit()
        chunker_ipc.commit()
        chunker_cite.commit()

        print(f'{ftag}: Parsed {i} patents')
    except Exception as e:
        print(f'{ftag}: EXCEPTION OCCURRED!')
        print_exc()

        chunker_grant.delete()
        chunker_ipc.delete()
        chunker_cite.delete()

# main entry point
def parse_many(files, output, threads=10, display=1_000, overwrite=False, dryrun=False):
    # needed for multiprocess
    global parse_file_opts

    # collect files
    if type(files) is str or isinstance(files, os.PathLike):
        file_list = (
            sorted(glob.glob(f'{files}/*.dat')) +
            sorted(glob.glob(f'{files}/pgb*.xml')) +
            sorted(glob.glob(f'{files}/ipgb*.xml'))
        )
    else:
        file_list = args.target

    # ensure output dir
    if not dryrun and not os.path.exists(output):
        print(f'Creating directory {output}')
        os.makedirs(output)

    # apply options
    def parse_file_opts(fpath):
        parse_file(fpath, output, display=display, overwrite=overwrite, dryrun=dryrun)

    # parse files
    with Pool(threads) as pool:
        pool.map(parse_file_opts, file_list, chunksize=1)
