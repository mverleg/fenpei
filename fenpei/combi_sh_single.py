
"""
A job that represents a series of subjobs whose results should be joined.
"""

from copy import copy
from itertools import product
from fenpei.job_sh_single import ShJobSingle
from fenpei.utils import compare_results


class CombiSingle(ShJobSingle):
	#todo: don't actually need the 'Sh'(shell) part, but that's how the hierarchy grew
	#todo: this is kind of a hybrid between a queue and a job, which is undesirable design

	def __init__(self, name, subs, ranges, child_cls, batch_name=None, child_kwargs=None,
			name_template=None, aggregation_func=None, **kwargs):
		child_kwargs = child_kwargs or {}
		self._child_cls = child_cls
		params = ranges.keys()
		combis = product(*ranges.values())
		weight = self._make_children(params, combis, subs, name_template=name_template, name=name,
			batch_name=batch_name, child_kwargs=child_kwargs)
		sub_range = copy(subs)
		sub_range.update(ranges)
		super(CombiSingle, self).__init__(name=name, subs=sub_range, batch_name=batch_name, weight=weight, **kwargs)
		self.aggregation_func = aggregation_func
		self.input = sub_range
	
	def get_input(self):
		return copy(self.input)
	
	def _make_children(self, params, combis, subs, name_template, name, batch_name, child_kwargs):
		self._child_jobs = []
		assert hasattr(combis, '__iter__')
		weight = 0
		for combi in combis:
			childsubs = copy(subs)
			for param, value in zip(params, combi):
				childsubs[param] = value
			if name_template is None:
				nameparts = []
				for param, value in zip(params, combi):
					nameparts.append('{1:s}{2:}'.format(name, param, value))
				childname = '_'.join([name, ''] + sorted(nameparts))
			else:
				childname = name_template.format(name=name, **childsubs)
			subjob = self._child_cls(name=childname, subs=childsubs, batch_name=batch_name, **child_kwargs)
			self._child_jobs.append(subjob)
			weight += subjob.weight
		return weight
	
	def compare_results(self, parameters=('name',), filter=None):
		return compare_results(self._child_jobs, parameters=parameters, filter=filter)
	
	def __repr__(self):
		return '{0:s}*{1:d}'.format(super(CombiSingle, self).__repr__(), len(self._child_jobs))

	def get_default_subs(self, version=1):
		return self._child_cls.get_default_subs(version=version)

	def _queue_children(self):
		"""
		Connect children to a queue one-way (not add them, just connect).
		"""
		for job in self._child_jobs:
			job.queue = self.queue
	
	def get_jobs(self):
		return tuple(self._child_jobs)
	
	@classmethod
	def get_files(cls):
		return []

	@classmethod
	def get_sub_files(cls):
		return []

	@classmethod
	def get_nosub_files(cls):
		return []

	@classmethod
	def run_file(cls):
		return None

	def is_prepared(self):
		self._queue_children()
		for job in self._child_jobs:
			if not job.is_prepared():
				self._log('{0:s} not prepared because {1:s} is not'.format(self, job), level=3)
				return False
		self._log('{0:s} prepared because all {1:d} children are'.format(self, len(self._child_jobs)), level=3)
		return True

	def is_started(self):
		self._queue_children()
		for job in self._child_jobs:
			if not job.is_started():
				self._log('{0:s} not started because {1:s} is not'.format(self, job), level=3)
				return False
		self._log('{0:s} started because all {1:d} children are'.format(self, len(self._child_jobs)), level=3)
		return True

	def is_running(self):
		self._queue_children()
		for job in self._child_jobs:
			if job.is_running():
				self._log('{0:s} running because {1:s} is'.format(self, job), level=3)
				return True
		self._log('{0:s} not running because not all {1:d} children are'.format(self, len(self._child_jobs)), level=3)
		return False

	def is_complete(self):
		self._queue_children()
		for job in self._child_jobs:
			if not job.is_complete():
				self._log('{0:s} not complete because {1:s} is not'.format(self, job), level=3)
				return False
		self._log('{0:s} complete because all {1:d} children are'.format(self, len(self._child_jobs)), level=3)
		return True

	def prepare(self, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.prepare(*args, verbosity=0, **kwargs)
		return cnt

	def start(self, node, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			if job.find_status() not in {job.RUNNING, job.COMPLETED, job.CRASHED}:
				cnt += job.start(node, *args, verbosity=0, **kwargs)
		return cnt

	def fix(self, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.fix(*args, verbosity=0, **kwargs)
		return cnt

	def kill(self, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.kill(*args, verbosity=0, **kwargs)
		return cnt

	def cleanup(self, skip_conflicts=False, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.cleanup(skip_conflicts=skip_conflicts, *args, verbosity=0, **kwargs)
		return cnt

	def result(self, *args, **kwargs):
		assert self.queue, 'cannot get results for {0:} since it doesn\'t have a queue'.format(self)
		if not self.is_complete():
			return None
		job_results = self.queue.result(jobs=self._child_jobs, parallel=False)
		if not job_results:
			return None
		if self.aggregation_func is None:
			return self.aggregate(job_results)
		return self.aggregation_func(job_results)

	def _crash_reason_if_crashed(self, verbosity=0, *args, **kwargs):
		self._queue_children()
		completed, crashed = 0, 0
		crashexample = None
		for job in self._child_jobs:
			# print(job, job.find_status())
			if not job.is_complete():
				completed += 1
			if job.find_status() == job.CRASHED:
				crashexample = crashexample or job
				crashed += 1
		crashinfo = '+' * completed + 'C' * crashed + '.' * (len(self._child_jobs) - completed - crashed)
		if crashexample:
			crashinfo = '{0:s}; example {1:}: "{2:s}"'.format(crashinfo, crashexample, crashexample._crash_reason_if_crashed(verbosity=verbosity))
		return crashinfo

	def aggregate(self, job_results):
		"""
		Combine results from child jobs. Used if `aggregation_func` argument is not provided.
		"""
		return {res['name']: res for res in job_results.values()}


