
from re import match
from settings import calc_dir
from time import time
from os import remove
from os.path import join, exists, split
from schedule.shell import run_cmds
from utility.filesys import link_else_copy, mkdirp


CRASHED, NONE, PREPARED, RUNNING, COMPLETED = -1, 0, 1, 2, 3
status_names = {-1: 'crashed', 0: 'nothing', 1: 'prepared', 2: 'running', 3: 'completed'}

class Job(object):
    
    queue = None
    node = None
    pid = None
    status = None
    ''' set a group_cls to report results together with another class (that has the same output format) '''
    group_cls = None
    
    ''' name should be unique - that's your job '''
    def __init__(self, name, weight = 1, batch_name = None):
        assert match('^[\w\._-]+$', name)
        self.name = name
        self.weight = weight
        self.cluster = None
        self.batch_name = batch_name
        if self.batch_name:
            self.directory = join(calc_dir, batch_name, name)
        else:
            self.directory = join(calc_dir, name)
        self.status = NONE
    
    def __repr__(self):
        return self.name
    
    ''' .queue is not always set, so have own logging function '''
    def log(self, txt, *args, **kwargs):
        if self.queue is None:
            if len(txt.strip()):
                print txt
            else:
                print '(empty)'
        else:
            self.queue.log(txt, *args, **kwargs)
    
    ''' save information about a running job to locate the process '''
    def save(self):
        assert self.node is not None
        assert self.pid is not None
        with open('%s/node_pid.job' % self.directory, 'w+') as fh:
            fh.write('%s\n%s\n%s\n%s' % (self.name, self.node, self.pid, str(time())))
        self.log('job %s saved' % self, level = 3)
    
    ''' remove the stored process details '''
    def unsave(self):
        try:
            remove('%s/node_pid.job' % self.directory)
        except IOError:
            pass
        self.log('job %s save file removed' % self.name, level = 3)
    
    ''' load process details '''
    def load(self):
        try:
            with open('%s/node_pid.job' % self.directory, 'r') as fh:
                lines = fh.read().splitlines()
                self.node = lines[1]
                self.pid = int(lines[2])
            self.log('job %s loaded' % self.name, level = 3)
            return True
        except IOError:
            self.log('job %s save file not found' % self, level = 3)
            return False
    
    def is_prepared(self):
        return exists(self.run_file())
    
    def is_started(self):
        if not self.is_prepared():
            return False
        l = self.load()
        return l
    
    def is_running(self):
        if not self.is_prepared():
            return False
        if self.pid is None:
            if not self.load():
                return False
        if not self.queue:
            raise Exception('cannot check if %s is running because it is not in a queue' % self)
        proc_list = self.queue.processes(self.node)
        try:
            return self.pid in [proc['pid'] for proc in proc_list]
        except KeyError:
            raise Exception('node %s for job %s no longer found?' % (self.node, self))
        return True
    
    def is_complete(self):
        if not self.is_prepared():
            return False
        return True
    
    def find_status(self):
        def check_status_indicators(self):
            if self.is_complete():
                return COMPLETED
            if self.is_started():
                if self.is_running():
                    return RUNNING
                else:
                    return CRASHED
            if self.is_prepared():
                return PREPARED
            return NONE
        self.status = check_status_indicators(self)
        return self.status
    
    ''' return the path to the Python file to run (which is then copied and ran or added to a queue or something) '''
    def run_template(self):
        return self.template_file
    
    ''' return the path to the link/copy of .run_template(), whether or not it exists '''
    def run_file(self):
        return '%s/%s' % (self.directory, split(self.run_template())[1])
    
    def prepare(self, *args, **kwargs):
        self.status = PREPARED
        if not self.is_prepared():
            if self.batch_name:
                mkdirp(join(calc_dir, self.batch_name))
            mkdirp(self.directory)
        link_else_copy(self.run_template(), self.run_file())
    
    ''' run the Python file returned by .start_file() and save process id etc; 
        child classes should either just change .start_file(), or manually store pid and call .save() '''
    def start(self, node, *args, **kwargs):
        if self.is_running() or self.is_complete():
            if not self.queue is None:
                if self.queue.force:
                    if self.is_running():
                        self.kill()
                else:
                    self.log('you are trying to restart a job that is running or completed; use restart (-e) to skip such jobs or -f to stop this warning')
                    exit()
        if not self.is_prepared():
            self.prepare()
        pid = self.queue.run_job(node= node, filepath = self.run_file())
        self.node = node
        self.pid = pid
        self.save()
        if self.is_running():
            self.STATUS = RUNNING
        self.log('starting %s on %s with pid %s' % (self, self.node, self.pid), level = 2)
        return True
    
    ''' some code that can be ran to fix jobs, e.g. after bugfixes or updates '''
    def fix(self, *args, **kwargs):
        return False
    
    def kill(self, *args, **kwargs):
        if self.is_running():
            assert self.node is not None
            assert self.pid is not None
            self.log('killing %s: %s on %s' % (self, self.pid, self.node), level = 2)
            self.queue.stop_job(node = self.node, pid = self.pid)
            #run_cmds_on(['kill %s' % self.pid], node = self.node, queue = self.queue)
            return True
        else:
            self.log('job %s not running' % self, level = 2)
            return False
    
    def cleanup(self, *args, **kwargs):
        if self.is_running() or self.is_complete():
            if not self.queue is None:
                if not self.queue.force:
                    self.log('you are trying to clean up a job that is running or completed; if you are sure you want to do this, use -f')
                    exit()
        cmds = [
            '/bin/rm -r %s &> /dev/null' % self.directory,
        ]
        run_cmds(cmds, queue = self.queue)
    
    def result(self, *args, **kwargs):
        return None
    
    @classmethod
    def summary(cls, jobs, *args, **kwargs):
        pass


