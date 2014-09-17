#!/usr/bin/python3

import sys
import yaml
from job_creation import *

import pprint
from pprint import pprint as P

## read yaml file
file_name = sys.argv[1]
with open(file_name, "r") as f:
    dataMap = yaml.safe_load(f)

## get collections
collections=None
if 'collections' in dataMap:
    collections=dataMap['collections']
else:
    print("no collections defined")


jobs = create_jobs(collections, dataMap)
run_jobs(jobs)
