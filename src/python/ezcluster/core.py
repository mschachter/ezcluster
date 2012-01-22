import os
import sys
import time
import string
import random
import subprocess
import tempfile
from copy import copy

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
    j = Job(command, num_cpus, expected_runtime, log_file_template=log_file_template)
    j.id = id
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


class Launcher():
    
    def __init__(self, image_name, keypair_name, instance_type='m1.small',
                 security_groups=['default'], num_instances=1,
                 num_jobs_per_instance=1):        
        
        self.conn = boto.ec2.connect_to_region(config.get('ec2', 'region'))
        
        imgs = self.conn.get_all_images([image_name])
        if len(imgs) < 1:
            raise ConfigException('Cannot locate image by name: %s' % image_name)
        self.image = imgs[0]
        
        self.sqs = boto.connect_sqs()
        qname = config.get('sqs', 'queue_name')
        self.queue = self.sqs.get_queue(qname)
        if self.queue is None:
            raise ConfigException('Cannot connect to SQS queue: %s' % qname)
        
        self.keypair_name = keypair_name
        self.security_groups = security_groups
        self.instance_type = instance_type
        self.num_instances = num_instances
        self.num_jobs_per_instance = num_jobs_per_instance        
        self.jobs = []

    def is_ssh_running(self, instance):
        host_str = '%s@%s' % (config.get('ec2', 'user'), instance.public_dns_name)
        ret_code = subprocess.call(['ssh', '-o', 'ConnectTimeout=15',
                                    '-o', 'StrictHostKeyChecking=no',
                                    '-i', config.get('ec2', 'keypair_file'),
                                    host_str, 'exit'])
        return ret_code == 0
        
    def scp(self, instance, src_file, dest_file):
        dest_str = '%s@%s:%s' % (config.get('ec2', 'user'), instance.public_dns_name, dest_file)
        cmds = ['scp', '-o', 'StrictHostKeyChecking=no',
                '-i', config.get('ec2', 'keypair_file'),
                src_file, dest_str]
        subprocess.call(cmds, shell=False)
        
    def scmd(self, instance, cmd):
        host_str = '%s@%s' % (config.get('ec2', 'user'), instance.public_dns_name)
        cmds = ['ssh', '-o', 'StrictHostKeyChecking=no',
                '-i', config.get('ec2', 'keypair_file'),
                host_str, '""%s""' % cmd]
        ret_code = subprocess.call(cmds, shell=False)
        return ret_code
        

    def add_batch_job(self, cmds, num_cpus=1, expected_runtime=-1, log_file_template=None):
        j = Job(cmds, num_cpus=num_cpus, expected_runtime=expected_runtime, log_file_template=log_file_template)
        self.jobs.append(j)
    
    def add_job(self, cmds, num_cpus=1, expected_runtime=-1, log_file_template=None):
        j = Job(cmds, num_cpus=num_cpus, expected_runtime=expected_runtime, log_file_template=log_file_template)
        self.post_job(j)

    def post_jobs(self, batch_id=None):
        if batch_id is None:
            batch_id = random_string(10)
        for k,j in enumerate(self.jobs):
            self.post_job(j, id=k)
                   
            
    def post_job(self, j, id=None):
        if id is None:
            id = len(self.jobs) + 100
        j.id = id
        ji = j.to_dict()
        ji['batch_id'] = 'None'            
        msg = self.queue.new_message(body=json.dumps(ji))
        self.queue.write(msg)
        
        
    def start_instances(self, timeout_after=600):
        
        instances_pending = []
        print 'Starting %d instances...' % self.num_instances
        for k in range(self.num_instances):
            res = self.image.run(key_name=self.keypair_name,
                                 security_groups=self.security_groups,
                                 disable_api_termination=False,
                                 instance_type=self.instance_type)
            if len(res.instances) < 1:
                raise ConfigException('Could not reserve instance for some reason...')
            instances_pending.append(res.instances[0])
        
        start_time = time.time()
        timed_out = False
        while len(instances_pending) > 0 and not timed_out:
            for k,inst in enumerate(instances_pending):
                if inst.update() == 'running' and self.is_ssh_running(inst):
                    self.initialize_instance(inst)
                    del instances_pending[k]
                    break
            time.sleep(5.0)
            timed_out = (time.time() - start_time) > timeout_after
            
        if timed_out:
            print 'Timed out! Only %d instances were started...' % \
                  (self.num_instances - len(instances_pending))
    
    def fill_template_and_scp(self, instance, params, template_file, dest_file):
        tpl = ScriptTemplate(template_file)
        str = tpl.fill(params)
        tfile = tempfile.NamedTemporaryFile('w', delete=False)
        tfile.write(str)
        tfile.close()
        self.scp(instance, tfile.name, dest_file)
        os.remove(tfile.name)
    
    def initialize_instance(self, instance):
        
        print 'Initializing instance: %s' % instance.public_dns_name
        
        #copy s3cfg file
        params = {}
        params['ACCESS_KEY'] = self.conn.access_key
        params['SECRET_KEY'] = self.conn.secret_key
        self.fill_template_and_scp(instance, params,
                                   os.path.join(SH_DIR, 's3cfg'),
                                   '/tmp/s3cfg')
        
        #copy files over to instance
        self.scp(instance, config.get('ec2', 'cert_file'), '/tmp/cert.pem')
        self.scp(instance, config.get('ec2', 'private_key_file'), '/tmp/private_key.pem')
        
        params = {}
        params['ACCESS_KEY'] = self.conn.access_key
        params['SECRET_KEY'] = self.conn.secret_key
        params['REGION_URL'] = 'http://%s' % self.conn.region.endpoint
        params['DNS_NAME'] = instance.public_dns_name
        params['INSTANCE_ID'] = instance.id
        params['NUM_JOBS_PER_INSTANCE'] = self.num_jobs_per_instance
        params['BUCKET'] = config.get('s3', 'bucket')
 
        self.fill_template_and_scp(instance, params,
                                   os.path.join(SH_DIR, 'start-daemon.sh'),
                                   '/tmp/start-daemon.sh')
        
        self.scmd(instance, 'chmod 500 /tmp/start-daemon.sh')        
        self.scmd(instance, 'source /tmp/start-daemon.sh')
                
    def launch(self):
        send_self_tgz_to_s3()
        self.post_jobs()        
        self.start_instances()
    

class Daemon():
    """ The daemon runs on a working EC2 instance """
        
    def __init__(self):
        
        self.conn = boto.ec2.connect_to_region(config.get('ec2', 'region'))
        
        #get DNS name
        self.dns_name = os.environ['EC2_DNS_NAME']
        if self.dns_name is None or len(self.dns_name) == 0:
            raise ConfigException('Could not obtain value from environment variable EC2_DNS_NAME')
        self.instance_id = os.environ['EC2_INSTANCE_ID']
        if self.instance_id is None or len(self.instance_id) == 0:
            raise ConfigException('Could not obtain value from environment variable EC2_INSTANCE_ID')

        #get instance
        res = self.conn.get_all_instances([self.instance_id])
        if len(res) == 0:
            raise ConfigException('No instance found for id %s' % self.instance_id)
        self.instance = res[0].instances[0]
        if 'NUM_JOBS_PER_INSTANCE' not in os.environ:
            print '# of jobs per instance not found from environment variable NUM_JOBS_PER_INSTANCE, defaulting to 1'
            self.num_jobs_per_instance = 1
        else:
            self.num_jobs_per_instance = int(os.environ['NUM_JOBS_PER_INSTANCE'])
        self.num_running_jobs = 0
        
        #connect to SQS
        self.sqs = boto.connect_sqs()
        qname = config.get('sqs', 'queue_name')
        self.queue = self.sqs.get_queue(qname)
        if self.queue is None:
            raise ConfigException('Cannot connect to SQS queue: %s' % qname)
        
        self.jobs = {}
        self.output_dir = '/tmp'
        
        
    def get_next_job(self, timeout_after=600):
        
        start_time = time.time()
        timed_out = False
        while not timed_out:
            msg = self.queue.read(10)
            if msg is not None:
                msg_data = json.loads(msg.get_body())
                if msg_data['type'] == 'job':
                    job_info = copy(msg_data)
                    self.queue.delete_message(msg)
                    return job_info
            time.sleep(5.0)
            timed_out = (time.time() - start_time) > timeout_after
                    
        return None

    def run(self):
        
        print 'Starting daemon...'
        while True:
            for j in self.jobs.values():
                self.update_job_status(j)
            
            if len(self.jobs) < self.num_jobs_per_instance:
                next_job = self.get_next_job()
                if next_job is None and self.num_running_jobs == 0:
                    break
                self.run_job(next_job)                
            time.sleep(30.0)
            
        print 'All jobs are done!'
                    
    def run_job(self, job_info):
        
        j = job_from_dict(job_info)
        j.batch_id = job_info['batch_id']
        
        log_file = os.path.join(self.output_dir, j.log_file_template % j.id)
        fd = open(log_file, 'w')
        j.fd = fd
        j.log_file = log_file
        
        proc = subprocess.Popen(j.cmds, stdout=fd, stderr=fd)
        j.proc = proc
                
        self.jobs[j.id] = j
        self.update_job_status(j, create=True)
        

    def update_job_status(self, j, create=False):
        
        if create:
            status_msg = {}
            status_msg['type'] = 'job_status'
            status_msg['job'] = j.to_dict()
            status_msg['status'] = 'running'
            status_msg['pid'] = j.proc.pid
            status_msg['instance'] = self.instance_id
            status_msg['started_on'] = time.time()
            status_msg['last_update'] = time.time()
            status_msg['local_log_file'] = j.log_file
            
            msg = self.queue.write(self.queue.new_message(body=json.dumps(status_msg)))
            j.msg = msg
        else:
            mb = json.loads(j.msg.get_body())
            #check for job completion
            ret_code = j.proc.poll()
            if ret_code is not None:
                print 'Job %d is complete with exit code %d' % (j.id, ret_code)
                j.fd.close()
                mb['status'] = 'finished'
                mb['ret_code'] = ret_code                
                del self.jobs[j.id]
            mb['last_update'] = time.time()
            j.msg.set_body(json.dumps(mb))
            self.queue.write(j.msg)
            
            #copy logfile to s3 bucket
            (rootdir, log_filename) = os.path.split(j.log_file) 
            bpath = config.get('s3', 'bucket')
            bsp = bpath.split('/')
            bucket_name = bsp[0]
            dest_file = '%s/logs/%s' % ('/'.join(bsp[1:]), log_filename)
            s3 = boto.connect_s3()
            bucket = s3.get_bucket(bucket_name)
            print 'Writing log file from %s to s3://%s/%s' % (j.log_file, bucket_name, dest_file)
            key = bucket.new_key(dest_file)
            key.set_contents_from_filename(j.log_file)
