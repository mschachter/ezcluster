import time
import hashlib

from ezcluster.core import *

class Launcher():
    """ Launcher takes a bunch of job specifications and posts them to an SQS queue, creating the instances it needs to run them.
    
        It can run in "batch" mode, where jobs are accumulated locally and posted all
        at once, using the add_batch_job and post_jobs methods, or it can post each
        job to the SQS queue individually, using the add_job method.
        
        It can also start instances, based on an image. See the constructor
        for arguments. It can do both of these things in tandem if you construct
        an image, add jobs in batch mode, and call launch(). Otherwise you can
        use the start_instances function and add_batch_job/post_jobs methods
        independently.
    """
    
    def __init__(self, image_name, keypair_name, instance_type='m1.small',
                 security_groups=['default'], num_instances=1,
                 num_jobs_per_instance=1, quit_when_done=True):
        
        self.conn = boto.ec2.connect_to_region(config.get('ec2', 'region'))
        
        imgs = self.conn.get_all_images([image_name])
        if len(imgs) < 1:
            raise ConfigException('Cannot locate image by name: %s' % image_name)
        self.image = imgs[0]
        
        self.sqs = boto.connect_sqs()
        qname = config.get('sqs', 'job_queue')
        self.job_queue = self.sqs.get_queue(qname)
        if self.job_queue is None:
            raise ConfigException('Cannot connect to SQS queue: %s' % qname)
        
        self.keypair_name = keypair_name
        self.security_groups = security_groups
        self.instance_type = instance_type
        self.num_instances = num_instances
        self.num_jobs_per_instance = num_jobs_per_instance        
        self.jobs = []
        self.application_script_file = None
        self.quit_when_done=quit_when_done

    def is_ssh_running(self, instance):
        host_str = '%s@%s' % (config.get('ec2', 'user'), instance.public_dns_name)
        ret_code = subprocess.call(['ssh', '-o', 'ConnectTimeout=15',
                                    '-o', 'StrictHostKeyChecking=no',
                                    '-i', config.get('ec2', 'keypair_file'),
                                    host_str, 'exit'], shell=False)
        return ret_code == 0
        
    def scp(self, instance, src_file, dest_file):
        dest_str = '%s@%s:%s' % (config.get('ec2', 'user'), instance.public_dns_name, dest_file)
        cmds = ['scp', '-o', 'StrictHostKeyChecking=no',
                '-i', config.get('ec2', 'keypair_file'),
                src_file, dest_str]
        subprocess.call(cmds, shell=False)
        
    def scmd(self, instance, cmd, use_shell=False, remote_output_file='/dev/null'):        
        host_str = '%s@%s' % (config.get('ec2', 'user'), instance.public_dns_name)        
        cstr = '""nohup %s >> %s 2>> %s < /dev/null &""' %\
                (cmd, remote_output_file, remote_output_file)
        cmds = ['ssh', '-o', 'StrictHostKeyChecking=no',
                '-i', config.get('ec2', 'keypair_file'),
                host_str, cstr]
        #print ' '.join(cmds)
        proc = subprocess.Popen(cmds, shell=False)        
        proc.communicate()
        ret_code = proc.poll()
        return ret_code
    
    
    def generate_job_id(self):
        s = '%0.6f' % time.time()
        md5 = hashlib.md5()
        md5.update(s)
        return md5.hexdigest()    
        
    def add_batch_job(self, cmds, num_cpus=1, expected_runtime=-1, log_file_template=None):
        """ Adds a job to the local queue, job will be posted to SQS queue with call to post_jobs """
        j = Job(cmds, num_cpus=num_cpus, expected_runtime=expected_runtime, log_file_template=log_file_template)
        self.jobs.append(j)
    
    def add_job(self, cmds, num_cpus=1, expected_runtime=-1, log_file_template=None, batch_id=None):
        """ Skips the local queue and posts job directly to SQS queue """ 
        j = Job(cmds, num_cpus=num_cpus, expected_runtime=expected_runtime, log_file_template=log_file_template)
        j.batch_id = batch_id
        self.post_job(j)

    def post_jobs(self, batch_id=None):
        """ Takes jobs in local queue and posts them to SQS queue """
        if batch_id is None:
            batch_id = random_string(10)
        for k,j in enumerate(self.jobs):
            j.batch_id = batch_id
            self.post_job(j, id=self.generate_job_id())
            
    def post_job(self, j, id=None):
        """ Posts a single job to SQS queue """
        if id is None:
            id = self.generate_job_id()            
        j.id = id
        ji = j.to_dict()        
        ji['batch_id'] = str(j.batch_id)            
        msg = self.job_queue.new_message(body=json.dumps(ji))
        self.job_queue.write(msg)

    def set_application_script(self, file_name):
        self.application_script_file = file_name
                
    def start_instances(self, timeout_after=1800):
        """ Starts the number of instances specified in the constructor.
        
            The steps it takes to do this are as follows:
            1) Start each instance with the specified security group, keypair, and insatnce type.
            2) Wait for each instance to be in a running state with a working SSH connection
            3) Initialize each instance by copying over some files and running a script (see initialize_instance)
        """ 
        
        send_self_tgz_to_s3()
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
        
        #give the startup a bit of time
        print 'Giving the startup 30 seconds...'
        time.sleep(30.0)
        
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
        """ Initialize a started EC2 instance. T
        
            The instance is assumed to have ec2-api-tools, s3cmd, and python
            installed, as well as a running SSH daemon. All uploaded files are
            stored in /tmp. The following steps are taken:
            
            1) Send over an s3cmd config file filled in the right keys to /tmp/s3cmd
            2) Send over cert and private key to /tmp/cert.pem and /tmp/private_key.pem
            3) Send over a shell script to /tmp/start-daemon.sh that sets the right
               environment variables, gets ezcluster installed from S3, and then starts
               a daemon.
            4) Start the shell script.
        """
        
        print 'Initializing instance: %s' % instance.public_dns_name
        
        #copy s3cfg file
        params = {}
        params['ACCESS_KEY'] = self.conn.access_key
        params['SECRET_KEY'] = self.conn.secret_key
        self.fill_template_and_scp(instance, params,
                                   os.path.join(SH_DIR, 's3cfg'),
                                   '~/.s3cfg')
        
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
        params['QUIT_WHEN_EMPTY'] = self.quit_when_done
         
        self.fill_template_and_scp(instance, params,
                                   os.path.join(SH_DIR, 'start-daemon.sh'),
                                   '/tmp/start-daemon.sh')
        self.scmd(instance, 'chmod 500 /tmp/start-daemon.sh')
        
        if self.application_script_file is not None:
            self.fill_template_and_scp(instance, params,
                                       self.application_script_file,
                                       '/tmp/application-script.sh')
            self.scmd(instance, 'chmod 500 /tmp/application-script.sh')
        
        self.scmd(instance, '/tmp/start-daemon.sh', remote_output_file='/tmp/ezcluster-daemon-startup.log')        
                
    def launch(self):
        self.post_jobs()        
        self.start_instances()
