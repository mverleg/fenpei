
from os.path import exists
from settings import python_dir
from numpy import ndarray
from re import compile
from pyfann.libfann import ERRORFUNC_TANH, SIGMOID_SYMMETRIC
from schedule.job_nn import NN_Job
from utility.load_data import write_array_to_columns
from fitting.check import fann_check


class FANN_Job(NN_Job):
    
    template_file = '%s/fitting/run_fann_fit.py' % python_dir
    
    def __init__(self, name, weight, data, layers, batch_name = None, use_scaling = True, learning_rate = .7, train_error_function = ERRORFUNC_TANH, activation_function_hidden = SIGMOID_SYMMETRIC, activation_function_output = SIGMOID_SYMMETRIC):
        super(FANN_Job, self).__init__(name = name, weight = weight, batch_name = batch_name, data = data, layers = layers)
        assert len(layers) >= 1
        assert isinstance(data, ndarray)
        assert exists(self.template_file)
        self.in_dimension = data.shape[-1] - 1
        self.use_scaling = use_scaling
        self.learning_rate = learning_rate
        self.train_error_function = train_error_function
        self.activation_function_hidden = activation_function_hidden
        self.activation_function_output = activation_function_output
    
    def is_prepared(self):
        return super(FANN_Job, self).is_prepared() and exists('%s/settings.in' % self.directory)
    
    def is_complete(self):
        return super(FANN_Job, self).is_complete() and exists('%s/fit.fann' % self.directory) and exists('%s/min_range.scale' % self.directory)
    
    def prepare(self, *args, **kwargs):
        if self.is_prepared():
            return False
        super(FANN_Job, self).prepare()
        with open('%s/settings.in' % self.directory, 'w+') as fh:
            fh.write('\n'.join([str(val) for val in int(self.use_scaling), self.learning_rate, self.train_error_function, self.activation_function_hidden, self.activation_function_output]))
        return True
    
    def fix(self, *args, **kwargs):
        if not exists('%s/predictions.coord' % self.directory):
            if exists('%s/fit.fann' % self.directory):
                self.log('fixing %s' % self, level = 2)
            else:
                self.log('%s not complete; can\'t fix' % self, level = 2)
                return False
            check_data = fann_check('%s/fit.fann' % self.directory, '%s/test.coord' % self.directory, '%s/min_range.scale' % self.directory)[0]
            write_array_to_columns(check_data, '%s/predictions.coord' % self.directory)
            return True
        return False
    
    def result(self, *args, **kwargs):
        if not self.is_complete():
            return None
        with open('%s/result.out' % self.directory, 'r') as fh:
            result_line = ' '.join(fh.read().splitlines()[-4:])
            result_pattern = '.* ([\s\d\.]+)\*([\s\d\.]+) iterations([\s\d\.]+e?\-?[\s\d\.]+)training error([\s\d\.]+e?\-?[\s\d\.]+)testing error.*'
            self.log('matching "%s" against "%s"' % (result_line, result_pattern), level = 3)
            found = compile(result_pattern).match(result_line)
            if not found:
                self.log('%s is complete but results could not be extracted (pattern does not match)' % self)
                return None
            matches = [float(val) for val in found.groups()]
        result_filename = '%s/fit.fann' % self.directory
        scale_filename = '%s/min_range.scale' % self.directory
        check_filename = '%s/predictions.coord' % self.directory
        return {
            'type': self.__class__,
            'job': self,
            'iterations': int(matches[0]) * int(matches[0]),
            'train_error': float(matches[2]),
            'test_error': float(matches[3]),
            'result_filename': result_filename,
            'scale_filename': scale_filename,
            'check_filename': check_filename,
        }


