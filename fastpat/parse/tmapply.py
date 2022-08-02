import re
import os
import glob
from collections import defaultdict
from traceback import print_exc
from multiprocessing import Pool
from lxml.etree import iterparse

from ..tools.parse import *
from ..tools.tables import ChunkWriter, DummyWriter

def parse_tmapply(elem, fname):
    tma = defaultdict(str)
    tma['file'] = fname

    # top-level section
    tma['serial'] = get_text(elem, 'serial-number')

    # header information
    head = elem.find('case-file-header')
    if head is not None:
        tma['regdate'] = get_text(head, 'registration-date')
        tma['filedate'] = get_text(head, 'filing-date')

    # case file statements
    stats = []
    for st in elem.findall('case-file-statements/case-file-statement'):
        tc = get_text(st, 'type-code')
        if tc.startswith('gs'):
            tx = get_text(st, 'text')
            stats.append((tc, tx))

    # summarize
    tma['n_stat'] = len(stats)
    tma['gs_codes'] = '; '.join([tc for tc, _ in stats])
    tma['statement'] = '; '.join([tx for _, tx in stats])

    # classifications
    uclas = []
    iclas = []
    for cl in elem.findall('classifications/classification'):
        uc = [u.text for u in cl.findall('us-code')]
        ic = [i.text for i in cl.findall('international-code')]
        uclas += uc
        iclas += ic
    tma['us_class'] = '; '.join(uclas)
    tma['int_class'] = '; '.join(iclas)

    # owners
    owns = []
    for ow in elem.findall('case-file-owners/case-file-owner'):
        nm = get_text(ow, 'party-name')
        owns.append(nm)
    tma['owners'] = '; '.join(dict.fromkeys(owns))

    # roll it in
    return tma

def store_tmapply(tma, chunker):
    chunker.insert(*(tma.get(k, '') for k in schema_tmapply))

# table schema
schema_tmapply = {
    'serial': 'str', # trademark number
    'regdate': 'str', # registration date
    'filedate': 'str', # filing date
    'n_stat': 'int', # number of GS statements
    'gs_codes': 'str', # all GS codes in statements
    'statement': 'str', # concatenated GS statements
    'us_class': 'str', # US classifications
    'int_class': 'str', # international classifications
    'owners': 'str', # all owner names
    'file': 'str', # path to source file
}

# file level
def parse_file(fpath, output, overwrite=False, dryrun=False, display=0):
    fdir, fname = os.path.split(fpath)
    ftag, fext = os.path.splitext(fname)

    opath_tmapply = os.path.join(output, f'tmapply_{ftag}.csv')

    if not overwrite and os.path.exists(opath_tmapply):
        print(f'{ftag}: Skipping')
        return

    if dryrun:
        chunker_tma = DummyWriter()
    else:
        chunker_tma = ChunkWriter(opath_tmapply, schema=schema_tmapply)

    # parse it up
    try:
        print(f'{ftag}: Starting')

        i = 0
        for event, elem in iterparse(fpath, tag='case-file', events=['end'], recover=True):
            i += 1

            # parse and store
            tma = parse_tmapply(elem, fname)
            store_tmapply(tma, chunker_tma)
            clear(elem)

            # output
            if display > 0 and i % display == 0:
                stma = {k: tma.get(k, '') for k in schema_tmapply}
                print(
                    'fn = {file:20.20s}, sn = {serial:10.10s}, rd = {regdate:10.10s}, '
                    'ic = {int_class:10.10s}, gs = {gs_codes:15.15s}, st = {statement:50.50s}, '
                    'ow = {owners:30.30s}'.format(**stma)
                )

        # commit to db and close
        chunker_tma.commit()

        print(f'{ftag}: Parsed {i} trademarks')
    except Exception as e:
        print(f'{ftag}: EXCEPTION OCCURRED!')
        print_exc()

        chunker_tma.delete()

# main entry point
def parse_many(files, output, threads=10, display=1_000, overwrite=False, dryrun=False):
    # needed for multiprocess
    global parse_file_opts

    # collect files
    if type(files) is str or isinstance(files, os.PathLike):
        file_list = sorted(glob.glob(f'{files}/apc*.xml'))
    else:
        file_list = files

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
