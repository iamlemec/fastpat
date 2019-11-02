#!/bin/env python3

import os
import sys
import time
import argparse

parser = argparse.ArgumentParser(description='fetch patent applications from USPTO bulk data')
parser.add_argument('--output', type=str, default='data/maint', help='directory to store fetched files')
parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')
parser.add_argument('--dryrun', action='store_true', help='just print commands that would be run')
args = parser.parse_args()

maint_url = 'https://bulkdata.uspto.gov/data/patent/maintenancefee/MaintFeeEvents.zip'

if args.dryrun:
    system = print
else:
    system = os.system

if not os.path.exists(args.output):
    os.makedirs(args.output)

zname = 'MaintFeeEvents.zip'
zpath = os.path.join(args.output, zname)

if args.overwrite or not os.path.isfile(zpath):
    print(f'Fetching {zname}')
    system(f'curl -o {zpath} {maint_url}')

opts = '' if args.overwrite else '-n'

print(f'Unzipping {zname}')
system(f'unzip {opts} {zpath} -d {args.output}')
