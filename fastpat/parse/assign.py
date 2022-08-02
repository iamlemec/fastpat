import re
import os
import glob
from collections import defaultdict
from traceback import print_exc
from lxml.etree import iterparse
from multiprocessing import Pool

from ..tools.parse import *
from ..tools.tables import ChunkWriter, DummyWriter

# parse assignment
def parse_assign_gen3(elem, fname):
    pat = defaultdict(str)
    pat['gen'] = 3
    pat['file'] = fname

    # top-level section
    record = elem.find('assignment-record')
    assignor = elem.find('patent-assignors')[0]
    assignee = elem.find('patent-assignees')[0]
    patents = elem.find('patent-properties')

    # assign id = reel-no + frame-no
    pat['bulkid'] = get_text(record, 'reel-no') + get_text(record, 'frame-no').zfill(3)

    # conveyance
    pat['conveyance'] = get_text(record, 'conveyance-text')

    # names
    pat['assignor'] = get_text(assignor, 'name')
    pat['assignee'] = get_text(assignee, 'name')

    # dates
    pat['execdate'] = get_text(assignor, 'execution-date/date')
    pat['recdate'] = get_text(record, 'recorded-date/date')

    # location
    pat['assignee_country'] = get_text(assignee, 'country-name', default='united states')
    pat['assignee_state'] = get_text(assignee, 'state')

    # patent info
    pat['patnums'] = [prune_patnum(pn) for pn in gen3_assign(patents)]

    return pat

# parse file
def parse_file_gen3(fpath):
    _, fname = os.path.split(fpath)
    for event, elem in iterparse(fpath, tag='patent-assignment', events=['end'], recover=True):
        yield parse_assign_gen3(elem, fname)
        clear(elem)

# table schema
schema_assign = {
    'assignid': 'str', # Assign ID
    'bulkid': 'str', # Group ID
    'patnum': 'str', # Patent number
    'execdate': 'str', # Execution date
    'recdate': 'str', # Reception date
    'conveyance': 'str', # Conveyance description
    'assignor': 'str', # Assignor name
    'assignee': 'str', # Assignee name
    'assignee_state': 'str', # State code
    'assignee_country': 'str', # Assignee country
    'gen': 'int', # USPTO data format
    'file': 'str', # path to source file
}

# chunking express
def store_patent(pat, chunker_assign):
    # filter out individuals and non-transfers
    pat['assignor_type'] = org_type(pat['assignor'])
    pat['assignee_type'] = org_type(pat['assignee'])
    pat['convey_type'] = convey_type(pat['conveyance'])
    if pat['assignor_type'] == ORG_INDV or pat['assignee_type'] == ORG_INDV or pat['convey_type'] == CONV_OTHER:
        return

    # store assign
    for i, pn in enumerate(pat['patnums']):
        pat['patnum'] = pn
        pat['assignid'] = pat['bulkid'] + '_' + str(i)
        chunker_assign.insert(*(pat[k] for k in schema_assign))

# file level
def parse_file(fpath, output, display=0, overwrite=False, dryrun=False):
    fdir, fname = os.path.split(fpath)
    ftag, fext = os.path.splitext(fname)

    opath_assign = os.path.join(output, f'assign_{ftag}.csv')

    if not overwrite:
        if os.path.exists(opath_assign):
            print(f'{ftag}: Skipping')
            return

    if dryrun:
        chunker_assign = DummyWriter()
    else:
        chunker_assign = ChunkWriter(opath_assign, schema=schema_assign)

    # parse it up
    try:
        print(f'{ftag}: Starting')

        i = 0
        for pat in parse_file_gen3(fpath):
            i += 1

            store_patent(pat, chunker_assign)

            # output
            if display > 0 and i % display == 0:
                pat['npat'] = len(pat['patnums'])
                print(
                    '[{npat:4d}]: {assignor:40.40s} [{assignor_type:1d}] -> '
                    '{assignee:30.30s} [{assignee_type:1d}] ({recdate:8.8s}, '
                    '{assignee_country:20.20s})'.format(**pat)
                )

        print(f'{ftag}: Parsed {i} records')

        # clear out the rest
        chunker_assign.commit()
    except Exception as e:
        print(f'{ftag}: EXCEPTION OCCURRED!')
        print_exc()

        chunker_assign.delete()

# main entry point
def parse_many(files, output, threads=10, display=1_000, overwrite=False, dryrun=False):
    # needed for multiprocess
    global parse_file_opts

    # collect files
    if type(files) is str or isinstance(files, os.PathLike):
        file_list = sorted(glob.glob(f'{files}/*.xml'))
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
