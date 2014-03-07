
from os.path import exists
from settings import calc_dir, fortran_dir, python_dir
from numpy import ndarray
from re import compile
from os.path import isfile
from schedule.shell import run_cmds
from schedule.job_nn import NN_Job
from fitting.check import dnn_check
from utility.load_data import write_array_to_columns


class DNN_Job(NN_Job):
    
    template_dir = '%s/nn' % fortran_dir
    template_file = '%s/fitting/run_dnn_fit.py' % python_dir
    
    def __init__(self, name, weight, data, layers, batch_name = None):
        super(DNN_Job, self).__init__(name = name, weight = weight, batch_name = batch_name, data = data, layers = layers)
        assert len(layers) == 2
        assert isinstance(data, ndarray)
        self.in_dimension = data.shape[-1] - 1
    
    def is_prepared(self):
        return super(DNN_Job, self).is_prepared() and exists('%s/nn.exe' % self.directory)
    
    def is_complete(self):
        if not super(DNN_Job, self).is_complete():
            return False
        try:
            with open('%s/result.out' % self.directory, 'r') as fh:
                if 'Total rmse_01=' in fh.read():
                    if exists('%s/srmse.txt' % self.directory):
                        return True
        except IOError:
            pass
        return False
    
    def prepare(self, *args, **kwargs):
        if self.is_prepared():
            return False
        super(DNN_Job, self).prepare()
        cmds = [
            'mkdir -p %s' % self.directory,
            'mkdir -p %s/result' % self.directory,
            'cd %s' % self.template_dir,
            'make', # nothing to be done in most cases
            'cd %s' % calc_dir,
            'ln -b %s/*.f %s' % (self.template_dir, self.directory),
            'ln -b %s/*.exe %s' % (self.template_dir, self.directory),
        ]
        with open('%s/input' % self.directory, 'w+') as fh:
            fh.write(self.input_content % {
                'in_dimension': self.in_dimension,
                'layer1': self.layers[0],
                'layer2': self.layers[1],
            })
        outp = run_cmds(cmds, queue = self.queue)
        if outp is None:
            raise Exception('job %s could not be prepared' % self)
        return True
    
    def fix(self, *args, **kwargs):
        if not exists('%s/predictions.coord' % self.directory):
            if exists('%s/result/W01.txt' % self.directory):
                self.log('fixing %s' % self, level = 2)
            else:
                self.log('%s not complete; can\'t fix' % self, level = 2)
                return False
            check_data = dnn_check('%s/result/W01.txt' % self.directory, '%s/test.coord' % self.directory)[0]
            write_array_to_columns(check_data, '%s/predictions.coord' % self.directory)
            return True
        return False
    
    def result(self, *args, **kwargs):
        if not self.is_complete():
            return None
        with open('%s/result.out' % self.directory, 'r') as fh:
            lines = fh.read().splitlines()
            result_line = ''.join(lines[-5:])
            result_pattern = '.*Epoch([\s\d]+)/1000; Mu=.*\s*Total rmse_01=([\d\s\.]+) meV; train=([\d\s\.]+); validation=([\d\s\.]+).*'
            self.log('matching "%s" against "%s"' % (result_line, result_pattern), level = 3)
            found = compile(result_pattern).match(result_line)
            if not found:
                if '********' in result_line:
                    self.log('%s error not in results file (too high); skipped' % self)
                else:
                    self.log('%s is complete but results could not be extracted (pattern does not match)' % self)
                return None
            matches = [float(val) for val in found.groups()]
        if not isfile('%s/result/W01.txt' % self.directory):
            self.log('%s is complete but results could not be extracted (weight file was not found)' % self)
            return None
        network_filename = '%s/result/W01.txt' % self.directory
        check_filename = '%s/predictions.coord' % self.directory
        with open('%s/srmse.txt' % self.directory, 'r') as fh:
            err_str_list = fh.read().splitlines()
            train_error, test_error = float(err_str_list[0]), float(err_str_list[1])
        return {
            'type': self.__class__,
            'job': self,
            'iterations': matches[0],
            'total_rmse': matches[1],
            'train_rmse': matches[2],
            'validate_rmse': matches[3],
            'train_error': train_error,
            'test_error': test_error,
            'result_filename': network_filename,
            'check_filename': check_filename,
        }
    
    input_content = """ %(in_dimension)d                               # NIN
 2                               # NHID - number of input neurons
 %(layer1)d                              # NL(1)~NL(NHID) - number of neurons in first hidden layer
 %(layer2)d                              # NL(1)~NL(NHID) - ... second ...
 0.90                            # RATIO
 1000                            # NLOOP - number of repetitions when training 
 1                               # NCYCLE - train one cycle (make more jobs for more cycles)
 train.coord                     # RE_FILE file containing the coordinates (columns)
"""


