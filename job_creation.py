#!/usr/bin/python3
from job import *

import pprint
from pprint import pprint as P

def get_job_info(default_dict, user_data_dict):
    """Merges info from defaults and user entry"""
    defaults={
            'type' : None,
            'home' : '/home/{user}',
            'rsync_flag': ['-avh'],
            'files' : None,
            'collections' : None
        }
    result=dict()

    #merge
    for item in ['type','home','rsync_flags']:
        if item in default_dict:
            result[item]=default_dict[item]
        if item in user_data_dict:
            result[item]=user_data_dict[item]
        if item not in result:
            result[item]=defaults[item]

    for item in ['files', 'collections']:
        if item in default_dict:
            result[item]=default_dict[item]
        if item in user_data_dict:
            if item not in result:
                result[item]=user_data_dict[item]
            else:
                result[item]+=user_data_dict[item]

        if item not in result:
            result[item]=defaults[item]

    return result


def build_rsync_jobs(host, user, job_dict):
    """Creates job instances"""
    jobs=[]

    for file_pair in job_dict['files']:
        jobs.append(
            rsync_job(
                luser = user, ruser = user,
                lhost = None, rhost = host,
                lhome = job_dict['home'], rhome = None,
                src   = file_pair[0], dest = file_pair[1] ,
                flags = job_dict['rsync_flags']
            )
        )
    return jobs

def build_jobs(host_list, user_list, job_dict, collections):
    jobs=[]
    if 'type' not in job_dict:
        print("no operation type give!!")
        return

    ## expand collections
    files=[]
    if job_dict['files']:
        files = job_dict['files']
    if job_dict['collections']:
        for collection in job_dict['collections']:
            files+=collections[collection]
    if len(files) > 0:
        job_dict['files'] = files

    ## select creation function depending on type
    fun=None
    if job_dict['type'] == 'rsync':
        fun=build_rsync_jobs
    else:
        print ("no valid operation type")

    ## run creation function for ever host user pair
    for host in host_list:
        for user in user_list:
            rv = fun(host, user, job_dict)
            jobs += rv
    return jobs

def create_jobs(dataMap):
    """Create Job list form yaml map"""

    ## get collections
    collections=None
    if 'collections' in dataMap:
        collections=dataMap['collections']
    else:
        print("no collections defined")

    jobs=[]
    if 'hosts' not in dataMap:
        print("no hosts defined!")
    else:
        for hosts_key, hosts_val in dataMap['hosts'].items():
            host_list=hosts_key.split(',')

            default=None
            if 'default' in hosts_val:
                default=hosts_val['default']
            else:
                print("defaults missing")

            if 'users' in hosts_val:
                for users_key,users_val in hosts_val['users'].items():
                    user_list=users_key.split(',')
                    job_dict = get_job_info(default, users_val)
                    job_list=build_jobs(host_list, user_list, job_dict, collections)
                    jobs += job_list
            else:
                print("no users given")
    return jobs
