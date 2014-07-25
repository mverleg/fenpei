
"""
	only meant as base class for neural nets
"""

from fenpei.job import Job
from os.path import exists
from numpy import ndarray
from pyfann.libfann import ERRORFUNC_TANH, SIGMOID_SYMMETRIC
from fitting.visualize_results import plot_errors_points, bar_error_minima,\
	plot_example_fits
from utility.data_train_test import split_train_test


class NNJob(Job):

	def __new__(cls, *args, **kwargs):
		cls.group_cls = NNJob
		return super(NNJob, cls).__new__(cls, *args, **kwargs)

	def __init__(self, name, weight, data, layers, batch_name = None, use_scaling = True, learning_rate = .7, train_error_function = ERRORFUNC_TANH, activation_function_hidden = SIGMOID_SYMMETRIC, activation_function_output = SIGMOID_SYMMETRIC):
		super(NNJob, self).__init__(name = name, weight = weight, batch_name = batch_name)
		assert len(layers) >= 1
		assert isinstance(data, ndarray)
		self.data = data
		self.layers = layers

	def is_prepared(self):
		return super(NNJob, self).is_prepared() and exists('%s/train.coord' % self.directory)

	def is_complete(self):
		return super(NNJob, self).is_complete() and exists('%s/predictions.coord' % self.directory)

	def prepare(self, *args, **kwargs):
		if self.is_prepared():
			return False
		super(NNJob, self).prepare()
		split_train_test(self.data, train = '%s/train.coord' % self.directory, test = '%s/test.coord' % self.directory)
		return True

	@classmethod
	def summary(cls, jobs, *args, **kwargs):
		''' get the results that return '''
		results = []
		complete_jobs = []
		for job in jobs:
			result = job.result()
			if result:
				complete_jobs.append(job)
				results.append(result)
		''' visualization functions '''
		if len(results):
			plot_errors_points(complete_jobs, results)
			bar_error_minima(complete_jobs, results)
			plot_example_fits(complete_jobs, results)


