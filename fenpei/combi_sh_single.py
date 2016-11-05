
"""
A job that represents a series of subjobs whose results should be joined.
"""

from copy import copy
from itertools import product
from fenpei.job_sh_single import ShJobSingle


class CombiSingle(ShJobSingle):
	#todo: don't actually need the 'Sh'(shell) part, but that's how the hierarchy grew

	def __init__(self, name, subs, ranges, child_cls, batch_name=None, child_kwargs=None, **kwargs):
		child_kwargs = child_kwargs or {}
		self._child_cls = child_cls
		self._child_jobs = []
		self.weight = 0
		params = ranges.keys()
		combis = product(*ranges.values())
		for combi in combis:
			childsubs = copy(subs)
			nameparts = []
			for param, value in zip(params, combi):
				childsubs[param] = value
				nameparts.append('{1:s}{2:s}'.format(name, param, '{0:02d}'.format(value) if isinstance(value, int) else value))
			childname = '_'.join([name, ''] + sorted(nameparts))
			subjob = child_cls(name=childname, subs=childsubs, batch_name=batch_name, **child_kwargs)
			self._child_jobs.append(subjob)
			self.weight += subjob.weight
		sub_range = copy(subs)
		sub_range.update(ranges)
		super(CombiSingle, self).__init__(name=name, subs=sub_range, batch_name=batch_name, weight=1, **kwargs)
	
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
				self._log('{0:s} not prepared because {1:s} is not'.format(self, job), level=2)
				return False
		self._log('{0:s} prepared because all {1:d} children are'.format(self, len(self._child_jobs)), level=2)
		return True

	def is_started(self):
		#todo: optimization by doing previously running jobs last
		self._queue_children()
		for job in self._child_jobs:
			if not job.is_started():
				self._log('{0:s} not started because {1:s} is not'.format(self, job), level=2)
				return False
		self._log('{0:s} started because all {1:d} children are'.format(self, len(self._child_jobs)), level=2)
		return True

	def is_running(self):
		#todo: optimization by doing previously running jobs last
		self._queue_children()
		for job in self._child_jobs:
			if job.is_running():
				self._log('{0:s} running because {1:s} is'.format(self, job), level=2)
				return True
		self._log('{0:s} not running because not all {1:d} children are'.format(self, len(self._child_jobs)), level=2)
		return False

	def is_complete(self):
		self._queue_children()
		for job in self._child_jobs:
			if not job.is_complete():
				self._log('{0:s} not complete because {1:s} is not'.format(self, job), level=2)
				return False
		self._log('{0:s} complete because all {1:d} children are'.format(self, len(self._child_jobs)), level=2)
		return True

	def prepare(self, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.prepare(*args, verbosity=0, **kwargs)
		return bool(cnt)
	
	def start(self, node, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.start(node, *args, verbosity=0, **kwargs)
		return bool(cnt)

	def fix(self, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.fix(*args, verbosity=0, **kwargs)
		return bool(cnt)

	def kill(self, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.kill(*args, verbosity=0, **kwargs)
		return bool(cnt)

	def cleanup(self, skip_conflicts=False, verbosity=0, *args, **kwargs):
		self._queue_children()
		cnt = 0
		for job in self._child_jobs:
			cnt += job.cleanup(skip_conflicts=skip_conflicts, *args, verbosity=0, **kwargs)
		return bool(cnt)

	def result(self, *args, **kwargs):
		if not self.is_complete():
			return None
		return None
		#todo: aggregate

	def _crash_reason_if_crashed(self, verbosity=0, *args, **kwargs):
		completed, crashed = 0, 0
		for job in self._child_jobs:
			print(job, job.find_status())
			if not job.is_complete():
				completed += 1
			if job._crash_reason_if_crashed():
				crashed += 1
		return '+' * completed + 'X' * crashed + '.' * (len(self._child_jobs) - completed - crashed)


