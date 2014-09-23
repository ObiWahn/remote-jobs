#!/usr/bin/python3
import subprocess
import logging
import glob
import os

logger = logging.getLogger('remote-jobs')

import pprint
from pprint import pprint as P

class Job(object):
    type_dict = {
        None      : 0,
        'rsync'   : 1,
        'rdiff'   : 2,
        'command' : 3
    }

    def __init__(self, luser, lhost, lhome = None):
        self.luser=luser
        self.lhost=lhost
        self.lhome=lhome
        self.local=True
        self.cmd_type=None
        self.failed=False
        self.hold=False

    def map_cmd_to_prio(self):
        return self.type_dict[self.cmd_type]

    def __lt__(self,other):
        t1 = self.lhost , self.luser,  self.map_cmd_to_prio()
        t2 = other.lhost, other.luser, other.map_cmd_to_prio()
        return t1 < t2

    def __eq__(self,other):
        t1 = self.lhost , self.luser,  Job.type_dict[self.cmd_type]
        t2 = other.lhost, other.luser, Job.type_dict[other.cmd_type]
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
        logger.error("get_command not implemented for: " + self.__class__.__name__)
        return []

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

class RemoteJob(Job):
    """Remote Job"""
    def __init__(self,
        luser, ruser,
        lhost, rhost,
        lhome = None, rhome = None,
        local = False
        ):

        super().__init__(luser, lhost, lhome)

        self.ruser=ruser
        self.rhost=rhost
        self.rhome=rhome
        self.local=local

        if lhost == rhost:
            self.local=True

        if not luser:
            self.luser=ruser

    def __lt__(self,other):
        t1 = self.rhost,  self.ruser,  self.lhost,  self.luser,  Job.type_dict[self.cmd_type]
        t2 = other.rhost, other.ruser, other.lhost, other.luser, Job.type_dict[other.cmd_type]
        return t1 < t2

    def __eq__(self,other):
        t1 = self.rhost,  self.ruser,  self.lhost,  self.luser,  Job.type_dict[self.cmd_type]
        t2 = other.rhost, other.ruser, other.lhost, other.luser, Job.type_dict[other.cmd_type]
        return t1 == t2

class RemoteJobSrcDest(RemoteJob):
    def __init__(self,
        luser, ruser, lhost, rhost, lhome, rhome, local,
        src=None, dest=None, glob=False, direction="local2remote"
        ):
        super().__init__(luser, ruser, lhost, rhost, lhome, rhome, local)

        self.src  = self.expand_vars(src, remote = False)
        self.dest = self.expand_vars(dest, remote = True)
        self.glob = glob
        self.direction = direction

        if glob:
            self.src = self.expand_glob(src)
        else:
            self.src = [self.src]

        if not self.src:
            logger.error("Source files could not be found: " + self.expand_vars(src))
            logger.error(self.get_command_string())
            self.hold=True

    def expand_glob(self, path):
        logger.debug(path)
        rv = glob.glob(path)
        logger.debug(pprint.pformat(rv))
        return rv

class RemoteJobRdiff(RemoteJobSrcDest):
    def __init__(self,
            luser, ruser, lhost, rhost, lhome, rhome, local,
            src=None, dest=None, glob=False, direction="remote2local",
            flags=['-v']
        ):
        super().__init__(
            luser, ruser, lhost, rhost, lhome, rhome, local,
            src, dest, glob, direction)
        self.cmd_type="rdiff"
        self.flags=flags


    def get_command(self):
        if self.local:
            cmd  = [ 'rdiff-backup' ]
            cmd += self.flags
            cmd += self.src
            cmd += [ self.dest ]
        else:
            cmd  = [ 'rdiff-backup' ]
            cmd += self.flags
            cmd += self.src
            cmd += [ self.ruser + "@" + self.rhost + ":" + self.dest ]
        return cmd

class RemoteJobRsync(RemoteJobSrcDest):
    def __init__(self,
            luser, ruser, lhost, rhost, lhome, rhome, local,
            src=None, dest=None, glob=True, direction="local2remote", flags=['-avh']
        ):
        super().__init__(
            luser, ruser, lhost, rhost, lhome, rhome, local,
            src, dest, glob, direction)
        self.cmd_type="rsync"
        self.flags=flags

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



def test_connection(job):
    cmd=None
    if job.local or 'DEBUG' in os.environ:
        return True
    if job.cmd_type in ['rsync']:
        cmd=['ssh',
             '-o', 'ConnectionAttempts=1',
             '-o', 'ConnectTimeout=1',
             job.ruser + '@' + job.rhost,
             'echo' ,'Connected to ' + job.rhost
            ]
    if cmd:
        if subprocess.call(cmd) != 0:
            return False
    return True

def run_jobs(job_list):
    """runs all jobs in a given list"""
    job_list.sort()

    host=None
    connection=False
    for job in job_list:
        if job.rhost != host:
            host=job.rhost
            logger.debug("HOST: {host}".format(host=host))
            connection=test_connection(job)
        if not job.hold and ( connection or job.local):
            job.execute()

    failed_jobs=[]
    for job in job_list:
        if job.failed:
            failed_jobs.append(job.get_command_string())

    if failed_jobs:
        print("The following jobs failed to execute:")
        for job in failed_jobs:
            print(job)
