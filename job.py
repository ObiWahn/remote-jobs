#!/usr/bin/python3
import subprocess
import logging
import glob
import os

logger = logging.getLogger('remote-jobs')

import pprint
from pprint import pprint as P

class RemoteJob(object):
    """
    Basic job

    Most of our jobs will require information about
    local and remote users!

    If no local user is given it will default to the
    remote ueser.
    """
    def __init__(self,
        luser, ruser,
        lhost, rhost,
        lhome, rhome,
        local
        ):
        self.luser=luser
        self.ruser=ruser
        self.lhost=rhost
        self.rhost=rhost
        self.lhome=lhome
        self.rhome=rhome
        self.local=local
        self.type=None
        self.failed=False
        self.hold=False

        if lhost == rhost:
            self.local=True

        if not luser:
            self.luser=ruser

    def __lt__(self,other):
        t1 = self.rhost, self.ruser
        t2 = other.rhost, other.ruser
        return t1 < t2

    def __eq__(self,other):
        t1 = self.rhost, self.ruser
        t2 = other.rhost, other.ruser
        return t1 == t2

    def execute(self):
        cmd = self.get_command()
        logger.info(" ".join(cmd))
        if 'DEBUG' not in os.environ:
            try:
                sub_out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
                if len(sub_out) >= 1:
                    print(sub_out.decode('utf-8'))
            except subprocess.CalledProcessError as e:
                self.failed=True
                logger.error("Failed to execute - there is probably a misconfiguration in the yaml")
            #os and other error
            except Exception as e:
                ## still continue on all other errors
                self.failed=True
                logger.exception("Serious Unknown Error - Report to Developer")

    def get_command_string(self):
        cmd = self.get_command()
        return " ".join(cmd)

    def get_command(self):
        return ["not implemented"]

    def expand_vars(self, string, remote = False):
        """Expand Variables - Ugly Code - TODO"""
        if remote:
            user=self.ruser
            home=self.rhome
        else:
            user=self.luser
            home=self.lhome

        s1 = string.format( home=home, user=user )
        s2 = s1.format( user=self.luser )
        return s2

class RemoteJobSrcDest(RemoteJob):
    def __init__(self,
        luser, ruser, lhost, rhost, lhome, rhome, local,
        src=None, dest=None, glob=False
        ):
        super().__init__(luser, ruser, lhost, rhost, lhome, rhome, local)

        self.src  = self.expand_vars(src, remote = False)
        self.dest = self.expand_vars(dest, remote = True)
        self.glob = glob

        if glob:
            self.src = self.expand_glob(src)
        else:
            self.src = [self.src]

    def expand_glob(self, path):
        logger.debug(path)
        rv = glob.glob(path)
        logger.debug(pprint.pformat(rv))
        return rv

class RemoteJobRdiff(RemoteJobSrcDest):
    def __init__(self,
            luser, ruser, lhost, rhost, lhome, rhome, local,
            src=None, dest=None, glob=False, flags=['-v']
        ):
        super().__init__(
            luser, ruser, lhost, rhost, lhome, rhome, local, 
            src, dest, glob)
        self.type="rdiff"
        self.flags=flags

        if not self.src:
            logger.error("Source files could not be found: " + self.expand_vars(src))
            logger.error(self.get_command_string())
            self.hold=True

    def get_command(self):
        if self.local:
            cmd  = [ 'rsync' ]
        else:
            cmd  = [ 'rsync' ]
        return cmd

class RemoteJobRsync(RemoteJobSrcDest):
    def __init__(self,
            luser, ruser, lhost, rhost, lhome, rhome, local,
            src=None, dest=None, glob=True, flags=['-avh']
        ):
        super().__init__(
            luser, ruser, lhost, rhost, lhome, rhome, local,
            src, dest, glob)
        self.type="rsync"
        self.flags=flags

        if not self.src:
            logger.error("Source files could not be found: " + self.expand_vars(src))
            logger.error(self.get_command_string())
            self.hold=True

    def get_command(self):
        if self.local:
            cmd  = [ 'rsync' ]
            cmd += self.flags
            cmd += self.src
            cmd += [ self.dest ]
        else:
            cmd  = [ 'rsync' ]
            cmd += self.flags
            cmd += self.src
            cmd += [ self.ruser + "@" + self.rhost + ":" + self.dest ]
        return cmd

def test_connection(j):
    cmd=None
    if j.type in ['rsync']:
        cmd=['ssh',
             '-o', 'ConnectionAttempts=1',
             '-o', 'ConnectTimeout=1',
             j.ruser + '@' + j.rhost,
             'echo' ,'Connected to ' + j.rhost
            ]

    if cmd:
        if subprocess.call(cmd) != 0:
            return False

    return True

def run_job(job, host=None, connection=None):
    if job.rhost != host:
        host=job.rhost
        logger.debug("HOST: {host}".format(host=host))
        connection=test_connection(job)

    if not job.hold and ( connection or job.local):
        job.execute()


def run_jobs(job_list):
    """runs all jobs in a given list"""
    job_list.sort()

    host=None
    connection=False

    for job in job_list:
        run_job(job, host, connection)

    failed_jobs=[]
    for job in job_list:
        if job.failed:
            failed_jobs.append(job.get_command_string())

    if failed_jobs:
        print("The following jobs failed to execute:")
        for job in failed_jobs:
            print(job)
