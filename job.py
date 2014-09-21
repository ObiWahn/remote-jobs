#!/usr/bin/python3
import pprint
from pprint import pprint as P
import logging
logger = logging.getLogger('remote-jobs')
import subprocess

class job(object):
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
        lhome, rhome
        ):
        self.luser=luser
        self.ruser=ruser
        self.lhost=rhost
        self.rhost=rhost
        self.lhome=lhome
        self.rhome=rhome
        self.local_mode=False
        self.type=None
        self.failed=False

        if lhost == rhost:
            self.local_mode=True

        if not luser:
            self.luser=ruser

    def __lt__(self,other):
        t1 = self.rhost, self.ruser
        t2 = other.rhost, other.ruser
        return t1 < t2

    def __eq__(self,ohter):
        t1 = self.rhost, self.ruser
        t2 = other.rhost, other.ruser
        return t1 == t2

    def execute(self):
        cmd = self.get_command()
        logger.info(" ".join(cmd))
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            self.failed=True
            logger.error("Failed to execute - there is probably a misconfiguration in the yaml")
        except Exception as e:
            self.failed=True
            logger.error("Serious Unknown Error - Report to Developer")

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

class rsync_job(job):
    def __init__(self,
        luser, ruser, lhost, rhost, lhome, rhome,
        src=None, dest=None, flags=['-avh']
        ):
        super().__init__(luser, ruser, lhost, rhost, lhome, rhome)
        self.type="rsync"
        self.src=self.expand_vars(src, remote = False)
        self.dest=self.expand_vars(dest, remote = True)
        self.flags=flags

    def get_command(self):
        cmd  = [ 'rsync' ]
        cmd += self.flags
        cmd += [ self.src, self.ruser + "@" + self.rhost + ":" + self.dest ]
        return cmd

def run_jobs(job_list):
    """runs all jobs in a given list"""
    job_list.sort()

    host=None
    for j in job_list:
        if j.rhost != host:
            host=j.rhost
            logger.debug("HOST: {host}".format(host=host))
        j.execute()

    print("The following jobs failed to execute:")
    for j in job_list:
        if j.failed:
            print(j.get_command_string())

