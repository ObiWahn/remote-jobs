#!/usr/bin/python3
import sys
import yaml
import logging
import pprint
from pprint import pprint as P
from os.path import dirname, join, abspath
from os import environ
import pytest

environ['DEBUG'] = 'true'

FIXTURE_DIRECTORY = abspath(join(dirname(__file__), "fixtures"))
MODULE_DIR        = abspath(join(dirname(__file__), ".."))

sys.path.insert(0, MODULE_DIR)
from job_creation import *

def test_create_jobs():
    file_name = join(FIXTURE_DIRECTORY, "test.yaml")

    with open(file_name, "r") as f:
        dataMap = yaml.safe_load(f)
    #assert dataMap == {}

    jobs = create_jobs(dataMap)
    #assert jobs == {}


@pytest.fixture
def simple_jobs(request):
    logger = logging.getLogger('remote-jobs')
    logger.setLevel(logging.INFO) # need to set level here
    file_name = join(FIXTURE_DIRECTORY, "test.yaml")
    with open(file_name, "r") as f:
        return create_jobs(yaml.safe_load(f))


def test_run_job(caplog, simple_jobs):
    caplog.set_level(logging.INFO)

    run_jobs(simple_jobs)
    resout = caplog.text()

    file_name = join(FIXTURE_DIRECTORY, "test.log")
    with open(file_name, "r") as f:
        assert set(resout.splitlines()) == set(f.read().splitlines())



## UNUSED EXAMPLE CODE

# def test_run_job(capfd, simple_jobs):
#     run_jobs(simple_jobs)
#     resout, reserr = capfd.readouterr()
#     assert resout == "df"
#     assert reserr == "df"

# def test_run_job(caplog, simple_jobs):
#     #caplog.set_level(logging.INFO)   #why does this not work
#     #caplog.at_level(logging.INFO)
#     run_jobs(simple_jobs)
#     for logger_, level_, message_ in caplog.record_tuples():
#         if level_ == logging.INFO:
#             pass
#             #assert "" == logger_
#     resout = caplog.text()
#     assert resout == "df"
