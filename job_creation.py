#!/usr/bin/python3
import pprint
from pprint import pprint as P
from job import *
import logging
logger = logging.getLogger('remote-jobs')

def map_name(from_, to_, *dicts_):
    """ maps from name to name in a list of dictionaries """
    for d in dicts_:
        if from_ in d:
            d[to_]=d[from_]

def get_job_info(default_dict, user_data_dict):
    """Merges info from defaults and user entry"""
    defaults={
            'type'        : None,
            'rsync_flag'  : ['-avh'],
            'files'       : None,
            'collections' : None,
            'luser'       : None,
            'ruser'       : None,
            'lhome'       : '/home/{user}',
            'rhome'       : '/home/{user}',
            'lhost'       : None,
            'rhost'       : None
        }
    result=dict()

    my_map_name=lambda from_, to_: map_name(from_, to_, default_dict, user_data_dict)
    my_map_name('home','lhome')
    my_map_name('host','rhost')

    #merge
    for item in ['type', 'home', 'rsync_flags',
                 'lhost', 'rhost', 'luser', 'ruser', 'lhome', 'rhome']:
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
    logger.debug("job_dict:\n" + pprint.pformat(job_dict))

    jobs=[]
    for file_pair in job_dict['files']:
        if job_dict['luser']:
            luser = job_dict['luser']
        else:
            luser = user

        jobs.append(
            rsync_job(
                luser = luser, ruser = user,
                lhost = job_dict['lhost'], rhost = host,
                lhome = job_dict['lhome'], rhome = job_dict['rhome'],
                src   = file_pair[0],      dest = file_pair[1] ,
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
        logger.error("no valid operation type")

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
        logger.info("no collections defined")

    jobs=[]
    if 'hosts' not in dataMap:
        logger.error("no hosts defined!")
    else:
        for hosts_key, hosts_val in dataMap['hosts'].items():
            host_list=hosts_key.split(',')

            default=None
            if 'default' in hosts_val:
                default=hosts_val['default']
            else:
                logger.info("defaults missing")

            if 'users' in hosts_val:
                for users_key,users_val in hosts_val['users'].items():
                    user_list=users_key.split(',')
                    job_dict = get_job_info(default, users_val)
                    job_list=build_jobs(host_list, user_list, job_dict, collections)
                    jobs += job_list
            else:
                logger.error("no users given")
    return jobs
