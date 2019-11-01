#!/bin/env python3

import os
import sys
import time
import argparse

parser = argparse.ArgumentParser(description='fetch patent applications from USPTO bulk data')
parser.add_argument('--output', type=str, default='data/maint', help='directory to store fetched files')
parser.add_argument('--overwrite', action='store_true', help='overwrite existing files')
args = parser.parse_args()

maint_url = 'https://bulkdata.uspto.gov/data/patent/maintenancefee/MaintFeeEvents.zip'
maint_fname = 'MaintFeeEvents.zip'

if not os.path.exists(args.output):
    os.makedirs(args.output)

maint_path = os.path.join(args.output, maint_fname)
if args.overwrite or not os.path.isfile(maint_path):
    print(f'Fetching {maint_fname}')
    os.system(f'curl -o {maint_path} {maint_url}')

# to extract:
# cd data/maint
# unzip MaintFeeEvents.zip
