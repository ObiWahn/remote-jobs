#!/usr/bin/python3
import sys
import yaml
import logging
#import colored_log
import pprint
from pprint import pprint as P
from os.path import dirname, join, abspath

import pytest

FIXTURE_DIRECTORY = abspath(join(dirname(__file__), "fixtures"))
MODULE_DIR        = abspath(join(dirname(__file__), ".."))

sys.path.insert(0, MODULE_DIR)
from job_creation import *


def test_create_jobs():
    file_name = join(FIXTURE_DIRECTORY, "test.yaml")

    with open(file_name, "r") as f:
        dataMap = yaml.safe_load(f)

    assert dataMap == {}

    jobs = create_jobs(dataMap)
    assert jobs == {}

    run_jobs(jobs)

@pytest.fixture
def simple_jobs(request):
    file_name = join(FIXTURE_DIRECTORY, "test.yaml")

    with open(file_name, "r") as f:
        dataMap = yaml.safe_load(f)

    return dataMap


def test_run_job(simple_jobs):
    
    run_jobs(simple_jobs)

    #assert 
