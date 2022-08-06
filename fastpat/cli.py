#!/usr/bin/env python3

import os
import sys
import fire
from importlib import resources
from pathlib import Path

from . import (
    fetch_many, concat_tables, parse_apply, parse_grant, parse_assign,
    parse_assign, parse_maint, parse_tmapply, parse_compustat, cluster_firms,
    prune_assign, aggregate_cites, merge_firms
)

# parser dispatcher
parsers = {
    'apply': parse_apply,
    'grant': parse_grant,
    'assign': parse_assign,
    'maint': parse_maint,
    'tmapply': parse_tmapply,
    'compustat': parse_compustat,
}

# get path with fallbacks
def get_path(path, env):
    if path is None:
        if env in os.environ:
            return Path(os.environ[env])
        else:
            return
    else:
        return Path(path)

# read list files
def get_lines(fpath):
    return [
        s.strip() for s in fpath.read_text().split('\n') if len(s) > 0
    ]

class Main:
    def __init__(self, datadir=None, metadir=None):
        self.datapath = get_path(datadir, 'FASTPAT_DATADIR')
        self.metapath = get_path(metadir, 'FASTPAT_METADIR')

        if self.datapath is None:
            print('Error: must specify "datadir" path')
            sys.exit()

        if self.metapath is None:
            self.metapath = resources.files('fastpat') / 'meta'

    def stat(self):
        print(f'datapath: {self.datapath}')
        print(f'metapath: {self.metapath}')

    def fetch(self, ftype, files=None, delay=10, overwrite=False, unzip=False, dryrun=False):
        if files is None:
            fpath = self.metapath / f'{ftype}_files.txt'
        else:
            fpath = Path(files)
        flist = get_lines(fpath)

        output = self.datapath / 'raw' / ftype
        fetch_many(flist, output, delay=delay, overwrite=overwrite, unzip=unzip, dryrun=dryrun)

    def parse(self, ftype, path=None, concat=True, overwrite=False, dryrun=False, threads=10):
        if path is None:
            path = self.datapath / 'raw' / ftype
        pardir = self.datapath / 'parsed' / ftype
        tabdir = self.datapath / 'tables'

        if ftype in parsers:
            parsers[ftype](path, pardir, overwrite=overwrite, dryrun=dryrun, threads=threads)
            if concat:
                concat_tables(pardir, tabdir, ftype)
        else:
            print(f'Error: unknown data source "{ftype}"')

    def firms(self, action, sources=None, compustat=False):
        tabdir = self.datapath / 'tables'

        if action == 'assign':
            prune_assign(tabdir)
        elif action == 'cluster':
            cluster_firms(tabdir, sources=sources)
        elif action == 'cites':
            aggregate_cites(tabdir)
        elif action == 'merge':
            merge_firms(tabdir, compustat=compustat)
        else:
            print(f'Error: unknown firm action "{action}"')

def main():
    fire.Fire(Main)
