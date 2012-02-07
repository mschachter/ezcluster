from ezcluster.core import *

logger = logging.getLogger('daemon')
logger.setLevel(logging.DEBUG)


class Daemon():
    """ The daemon runs on a working EC2 instance. It collects jobs from the SQS queue and runs them. """
        
    def __init__(self):
        
        self.output_dir = '/tmp'        
        log_file = os.path.join(self.output_dir, 'ezcluster-daemon.log')
        lh = logging.FileHandler(log_file)
        logger.addHandler(lh)
        
        logger.debug('Initializing daemon...')
        
        self.conn = boto.ec2.connect_to_region(config.get('ec2', 'region'))
        
        #get DNS name
        self.dns_name = os.environ['EC2_DNS_NAME']
        if self.dns_name is None or len(self.dns_name) == 0:
            estr = 'Could not obtain value from environment variable EC2_DNS_NAME'
            logger.error(estr)
            raise ConfigException(estr)
        self.instance_id = os.environ['EC2_INSTANCE_ID']
        if self.instance_id is None or len(self.instance_id) == 0:
            estr = 'Could not obtain value from environment variable EC2_INSTANCE_ID'
            logger.error(estr)
            raise ConfigException(estr)

        #get instance
        res = self.conn.get_all_instances([self.instance_id])
        if len(res) == 0:
            estr = 'No instance found for id %s' % self.instance_id
            logger.error(estr)
            raise ConfigException(estr)
        self.instance = res[0].instances[0]
        if 'NUM_JOBS_PER_INSTANCE' not in os.environ:            
            logger.info('# of jobs per instance not found from environment variable NUM_JOBS_PER_INSTANCE, defaulting to 1')
            self.num_jobs_per_instance = 1
        else:
            self.num_jobs_per_instance = int(os.environ['NUM_JOBS_PER_INSTANCE'])
        self.num_running_jobs = 0
        
        #connect to SQS
        self.sqs = boto.connect_sqs()
        qname = config.get('sqs', 'job_queue')
        self.job_queue = self.sqs.get_queue(qname)
        if self.job_queue is None:
            estr = 'Cannot connect to SQS jobs queue: %s' % qname
            logger.error(estr)
            raise ConfigException(estr)
        
        qname = config.get('sqs', 'status_queue')
        self.status_queue = self.sqs.get_queue(qname)
        if self.status_queue is None:
            estr = 'Cannot connect to SQS status queue: %s' % qname
            logger.error(estr)
            raise ConfigException(estr)
        
        #do some s3 stuff         
        bpath = config.get('s3', 'bucket')
        bsp = bpath.split('/')
        self.bucket_name = bsp[0]
        self.bucket_path = ','.join(bsp[1:])
        self.s3_conn = boto.connect_s3()
        self.bucket = self.s3_conn.get_bucket(self.bucket_name)
        
        self.jobs = {}        
        logger.info('Daemon initialized and started on instance %s' % self.instance_id)
        logger.info('DNS name: %s' % self.dns_name)
        logger.info('# of jobs per instance: %d' % self.num_jobs_per_instance)
        logger.info('SQS job queue name: %s' % self.job_queue.name)
        logger.info('SQS status queue name: %s' % self.status_queue.name)
        logger.info('S3 path: %s/%s' % (self.bucket_name, self.bucket_path))
                
        
    def get_next_job(self, timeout_after=30.0, sleep_time=1.00, msg_hold_time=15):
        """ Get the next available job in the SQS queue. """        
        
        logger.debug('Looking for next job...')
        start_time = time.time()
        timed_out = False
        while not timed_out:
            msg = self.job_queue.read(msg_hold_time)
            if msg is not None:
                msg_data = json.loads(msg.get_body())
                #logger.debug('Got a message of type %s' % msg_data['type'])
                if msg_data['type'] == 'job':
                    job_info = copy(msg_data)
                    self.job_queue.delete_message(msg)
                    logger.debug('Got job from batch %s with id %d, deleted work order from queue.' %\
                                  (job_info['batch_id'], job_info['id']))
                    return job_info
            time.sleep(sleep_time)
            timed_out = (time.time() - start_time) > timeout_after
        logger.debug('No jobs found, timing out returning None...')
        return None

    def run(self, quit_when_empty=False, timeout_after=30.0, sleep_time=60.0):
        """ Run until there are no jobs left to run """        
        
        logger.debug('Starting daemon...')
        start_time = time.time()
        next_job=None
        while True:
            if len(self.jobs) > 0:
                logger.debug('Updating job statuses..')
            for j in self.jobs.values():
                self.update_job_status(j)
        
            if len(self.jobs) > 0:    
                logger.debug('# of running jobs: %d' % len(self.jobs))            
            while len(self.jobs) < self.num_jobs_per_instance:
                if next_job is None:
                    next_job = self.get_next_job()
                if next_job is None:
                    break
                start_time = time.time()
                self.run_job(next_job)
                next_job=None
            time.sleep(sleep_time)
            if next_job is None:
                next_job=self.get_next_job()
            timed_out=False
            if next_job is None:
                timed_out = (time.time() - start_time) > timeout_after
            else:
                start_time = time.time()
            if quit_when_empty and timed_out:
                break
        logger.debug('No jobs found, waiting for current jobs to complete...')
        for j in self.jobs.values():
            status_msg = self.create_status_message(j)
            if not status_msg['status'] == 'finished':
                j.proc.wait()
        logger.debug('All jobs completed, shutting down instance...')
        self.instance.terminate()

    def run_job(self, job_info):
        """ Run an individual job from the SQS queue. """
        
        j = job_from_dict(job_info)
        j.batch_id = job_info['batch_id']
        logger.debug('Starting job from batch %s with id %d' % (j.batch_id, j.id))
        
        log_file = os.path.join(self.output_dir, j.log_file_template % j.id)
        print 'Opening log file: %s' % log_file
        fd = open(log_file, 'w')
        j.fd = fd
        j.log_file = log_file
        logger.debug('Job log file: %s' % j.log_file)
        
        proc = subprocess.Popen(j.cmds, stdout=fd, stderr=fd)
        j.proc = proc
        logger.debug('Job command: %s' % ' '.join(j.cmds))
        logger.debug('Process started with pid=%d' % j.proc.pid)
                
        self.jobs[j.id] = j
        self.update_job_status(j, is_new=True)
        

    def update_job_status(self, j, is_new=False):
        """ Update a job status in the SQS queue. """
        
        status_msg = self.create_status_message(j, is_new=is_new)
        
        write_to_queue = True
        if not is_new:
            if status_msg['status'] == 'finished':
                logger.debug('Job %d is complete with exit code %d' % (j.id, status_msg['ret_code']))
                j.fd.close()
                del self.jobs[j.id]
                
                #copy logfile to s3 bucket
                (rootdir, log_filename) = os.path.split(j.log_file)
                dest_file = os.path.join(self.bucket_path, 'logs', log_filename)
                key = self.bucket.new_key(dest_file)
                key.set_contents_from_filename(j.log_file)
                logger.debug('Copied log file from %s to s3://%s/%s' % (j.log_file, self.bucket_name, dest_file))
                
            else:
                write_to_queue = False
                logger.debug('Job %d is still running...' % j.id)
                
        if write_to_queue:
            msg = self.status_queue.write(self.status_queue.new_message(body=json.dumps(status_msg)))
            j.msg = msg
            

    def create_status_message(self, j, is_new=False):
        status_msg = {}
        status_msg['type'] = 'job_status'
        status_msg['job'] = j.to_dict()        
        status_msg['instance'] = self.instance_id
        status_msg['started_on'] = time.time()
        status_msg['last_update'] = time.time()
        status_msg['local_log_file'] = j.log_file
        status_msg['pid'] = j.proc.pid
        status_msg['status'] = 'running'
        
        if not is_new:
            #check for job completion
            ret_code = j.proc.poll()
            if ret_code is not None:                                
                status_msg['status'] = 'finished'
                status_msg['ret_code'] = ret_code
            
        return status_msg


if __name__ == '__main__':
    quit_when_empty=False
    if len(sys.argv)>1:
        quit_when_empty=(sys.argv[1]=='True')
    d = Daemon()
    d.run(quit_when_empty=quit_when_empty)
