#!/usr/bin/python3
import pprint
from pprint import pprint as P

class job(object):
    def __init__(self,luser, ruser, lhost, rhost, lhome, rhome):
        self.luser=luser
        self.ruser=ruser
        self.lhost=ruser
        self.rhost=rhost
        self.lhome=lhome
        self.rhome=rhome
        self.type=None

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
        print(" ".join(cmd))

    def get_command(self):
        return ["not implemented"]

    def expand_vars(self, string, remote = False):
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
    def __init__(self, luser, ruser, lhost, rhost, lhome, rhome, src=None, dest=None, flags=['-avh']):
        super().__init__(luser, ruser, lhost, rhost, lhome, rhome)
        self.type="rsync"
        self.src=self.expand_vars(src, remote = False)
        self.dest=self.expand_vars(dest, remote = True)
        self.flags=flags

    def get_command(self):
        cmd =  [ 'rsyc' ]
        cmd +=  self.flags
        cmd += [ self.src, self.ruser + "@" + self.rhost + ":" + self.dest ]
        return cmd


def run_jobs(job_list):
    job_list.sort()

    host=None
    for j in job_list:
        if j.rhost != host:
            host=j.rhost
            print("HOST: {host}".format(host=host))
        j.execute()
