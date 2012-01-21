import os
import time
import string
import random
import subprocess
import tempfile

import boto.ec2
import simplejson as json

from ezcluster.config import config, ConfigException, SH_DIR

def random_string(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

class Job():
    def __init__(self, cmds, num_cpus, expected_runtime):
        self.id = None
        self.cmds = cmds        
        self.num_cpus = num_cpus
        self.expected_runtime = expected_runtime          
        
    def to_dict(self):        
        return {'type':'job',
                'id':self.id,
                'command':self.cmds,
                'num_cpus':self.num_cpus,
                'expected_runtime':self.expected_runtime}

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
                 security_groups=['default'], num_instances=1):        
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
        self.jobs = []

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
        subprocess.call(cmds, shell=False)
        

    def add_job(self, cmds, num_cpus=1, expected_runtime=-1):
        j = Job(cmds, num_cpus=num_cpus, expected_runtime=expected_runtime)
        self.jobs.append(j)    

    def post_jobs(self, batch_id=None):
        if batch_id is None:
            batch_id = random_string(10)
        for k,j in enumerate(self.jobs):
            j.id = k
            ji = j.to_dict()
            ji['batch_id'] = batch_id            
            msg = self.queue.new_message(body=json.dumps(ji))
            self.queue.write(msg)        
        
    def start_instances(self):
        
        instances_pending = []
        instances_running = []
        for k in self.num_instances:
            res = self.image.run(key_name=self.keypair_name,
                                 security_groups=self.security_groups,
                                 disable_api_termination=False,
                                 instance_type=self.instance_type)
            if len(res.instances) < 1:
                raise ConfigException('Could not reserve instance for some reason...')
            instances_pending.append(res.instances[0])
        
        while len(instances_pending) > 0:
            for k,inst in enumerate(instances_pending):
                if inst.update() == 'running':
                    self.intialize_instance(inst)
                    instances_running.append(inst)                    
                    del instances_pending[k]
                    break
            time.sleep(5.0)
    
    def initialize_instance(self, instance):
        
        #copy files over to instance
        self.scp(instance, config.get('ec2', 'cert_file'), '/tmp/cert.pem')
        self.scp(instance, config.get('ec2', 'private_key_file'), '/tmp/private_key.pem')
        
        params = {}
        params['ACCESS_KEY'] = self.conn.access_key
        params['SECRET_KEY'] = self.conn.secret_key
        params['REGION_URL'] = 'http://%s' % self.conn.region.endpoint      
        
        stpl = ScriptTemplate(os.path.join(SH_DIR, 'start-instance.sh'))
        sh_str = stpl.fill(params)
        
        tfile = tempfile.NamedTemporaryFile('w', delete=False)
        tfile.write(sh_str)
        tfile.close()
        self.scp(instance, tfile.name, '/tmp/start-instance.sh')
        os.remove(tfile.name)
        
        self.scmd(instance, 'chmod 500 /tmp/start-instance.sh')        
        self.scmd(instance, 'sh /tmp/start-instance.sh')
        
        
    def launch(self):
        self.post_jobs()        
        self.start_instances()
    

class Daemon():
    
    def __init__(self):
        pass
        
    
        
        
        