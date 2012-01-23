import os
import sys
import time
import string
import random
import logging
import tempfile
import subprocess
from copy import copy

import numpy as np

import boto.ec2
import simplejson as json

from ezcluster.config import config, ConfigException, SH_DIR, ROOT_DIR

def random_string(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def create_self_tgz():
    base_dir = os.path.abspath(os.path.join(ROOT_DIR, '..'))
    (tfd, temp_name) = tempfile.mkstemp(suffix='.tgz', prefix='ezcluster-')
    ret_code = subprocess.call(['tar', 'cvzf', temp_name,
                                '-C', base_dir,
                                '--exclude=.git',
                                'ezcluster'])
    if ret_code != 0:
        print 'create_self_tgz failed for some reason, ret_code=%d' % ret_code
    return temp_name

def send_self_tgz_to_s3():
      
    tgz_file = create_self_tgz()
    
    bpath = config.get('s3', 'bucket')
    bsp = bpath.split('/')
    bucket_name = bsp[0]
    dest_file = '%s/ezcluster.tgz' % '/'.join(bsp[1:])
    s3 = boto.connect_s3()
    bucket = s3.get_bucket(bucket_name)
    key = bucket.new_key(dest_file)
    key.set_contents_from_filename(tgz_file)
    os.remove(tgz_file)
    

class Job():
    def __init__(self, cmds, num_cpus, expected_runtime, log_file_template='job_%d.log'):
        self.id = None
        self.cmds = cmds        
        self.num_cpus = num_cpus
        self.expected_runtime = expected_runtime
        self.log_file_template = log_file_template          
        
    def to_dict(self):        
        return {'type':'job',
                'id':self.id,
                'command':self.cmds,
                'num_cpus':self.num_cpus,
                'expected_runtime':self.expected_runtime,
                'log_file_template':self.log_file_template}
    
def job_from_dict(ji):
    id = ji['id']
    command = ji['command']
    num_cpus = int(ji['num_cpus'])
    expected_runtime = ji['expected_runtime']
    log_file_template = ji['log_file_template']
    batch_id = 'None'
    if 'batch_id' in ji:
        batch_id = ji['batch_id']
    log_file = 'None'
    if 'local_log_file' in ji:
        log_file = ji['local_log_file']
    j = Job(command, num_cpus, expected_runtime, log_file_template=log_file_template)
    j.id = id
    j.batch_id = batch_id
    j.log_file = log_file
    return j


class ScriptTemplate:

    def __init__(self, fname):
        
        if not os.path.isfile(fname):
            print 'Template file does not exist: %s' % fname
            return
        f = open(fname, 'r')
        self.template = f.read()
        f.close()

    def fill(self, params):

        script_str = self.template
        for name,val in params.iteritems():
            nStr = '#%s#' % name
            script_str = string.replace(script_str, nStr, str(val))

        return script_str
