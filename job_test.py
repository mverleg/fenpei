
from fenpei.job import Job, RUNNING
from settings import fortran_dir
from fenpei.shell import run_cmds, run_cmds_on


class Test_Job(Job):
    
    queue = None
    template_dir = '%s/nn' % fortran_dir
    
    def __init__(self, name, weight, batch_name = None):
        super(Test_Job, self).__init__(name = name, weight = weight, batch_name = batch_name)
    
    def is_complete(self):
        try:
            with open('%s/result.txt' % self.directory, 'r') as fh:
                if any('10' in line for line in fh.readlines()):
                    return True
                else:
                    return False
        except IOError:
            return False
    
    def prepare(self, *args, **kwargs):
        if self.is_prepared():
            return
        super(Test_Job, self).prepare()
        with open('%s/test.sh' % self.directory, 'w+') as fh:
            fh.write(self.test_script)
        outp = run_cmds(['chmod u+x %s/test.sh' % self.directory], queue = self.queue)
    
    def start(self, node, *args, **kwargs):
        super(Test_Job, self).start(node, *args, **kwargs)
        cmds = [
            'cd %s' % self.directory,
            'nohup ./test.sh &> /dev/null &',
            'echo "$\!"'
        ]
        outp = run_cmds_on(cmds, node = node, queue = self.queue)
        if not outp:
            raise Exception('job %s could not be started' % self)
        assert int(outp[-1]) > 0 # to see if it's an integer
        self.pid = int(outp[-1])
        if self.is_running():
            self.status = RUNNING
        self.log('starting %s on %s with pid %d' % (self, self.node, self.pid), level = 2)
        self.save()
    
    test_script = """ for K in {1..10}
do
    sleep 1;
    echo "step $K" >> result.txt;
done
"""

