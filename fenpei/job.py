
"""
	Base class for fenpei job; this should be considered abstract.

	Your custom job(s) should inherit from this job and extend the relevant methods, such as::

	* is_prepared
	* is_complete
	* prepare
	* start
	* result
	* summary

	:: comment: make references
"""

from re import match
from sys import stdout
from bardeen.system import mkdirp
from time import time
from os import remove
from os.path import join, isdir
from shutil import rmtree
from .conf import CALC_DIR


class Job(object):

	CRASHED, NONE, PREPARED, RUNNING, COMPLETED = -1, 0, 1, 2, 3
	status_names = {-1: 'crashed', 0: 'nothing', 1: 'prepared', 2: 'running', 3: 'completed'}

	queue = None
	node = None
	pid = None
	status = None
	''' Set a group_cls to report results together with another class (that has the same output format). '''
	group_cls = None

	def __init__(self, name, weight = 1, batch_name = None):
		"""
			Create a Job object.

			:param name: unique name consisting of letters, numbers, dot (.) and underscore (_) **YOU need to make sure that name is unique (bijectively maps to job)**
			:param weight: the relative resource use of this job (higher relative weights means fewer jobs will be scheduled together)
			:param batch_name: optionally, a name of the same format as ``name``, which specifies the batch (will be grouped)
		"""
		assert match(r'^\w[/\w\.\+_-]*$', name), 'This is not a valid name: "{0:}"'.format(name)
		assert weight > 0
		self.name = name
		self.weight = weight
		self.cluster = None
		self.batch_name = batch_name
		if self.batch_name:
			assert match('^\w[\w\._-]*$', batch_name)
			self.directory = join(CALC_DIR, batch_name, name)
		else:
			self.directory = join(CALC_DIR, name)
		self.status = self.NONE

	def __repr__(self):
		if hasattr(self, 'name'):
			return self.name
		return '{0:s} id{1:}'.format(self.__class__.__name__, id(self))

	def _log(self, txt, *args, **kwargs):
		"""
			Logging function.
			.queue is not always set, so have own logging function.
		"""
		if self.queue is None:
			if len(txt.strip()):
				stdout.write(txt + '\n')
			else:
				stdout.write('(empty)\n')
		else:
			self.queue._log(txt, *args, **kwargs)

	def save(self):
		"""
			Save information about a running job to locate the process.
		"""
		assert self.node is not None
		assert self.pid is not None
		with open('%s/node_pid.job' % self.directory, 'w+') as fh:
			fh.write('%s\n%s\n%s\n%s' % (self.name, self.node, self.pid, str(time())))
		self._log('job %s saved' % self, level = 3)

	def unsave(self):
		"""
			Remove the stored process details.
		"""
		try:
			remove('%s/node_pid.job' % self.directory)
		except IOError:
			pass
		self._log('job %s save file removed' % self.name, level = 3)

	def load(self):
		"""
			Load process details from cache.
		"""
		try:
			with open('%s/node_pid.job' % self.directory, 'r') as fh:
				lines = fh.read().splitlines()
				self.node = lines[1]
				self.pid = int(lines[2])
			self._log('job %s loaded' % self.name, level = 3)
			return True
		except IOError:
			self._log('job %s save file not found' % self, level = 3)
			return False

	def is_prepared(self):
		pass

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
			return self.pid in [proc['pid'] for proc in proc_list if proc is not None]
		except KeyError:
			raise Exception('node %s for job %s no longer found?' % (self.node, self))

	def is_complete(self):
		"""
			Check if job completed succesfully.

			Needs to be extended by child class.
		"""
		if not self.is_prepared():
			return False
		return True

	def find_status(self):
		"""
			Find status using is_* methods.
		"""
		def check_status_indicators(self):
			if self.is_complete():
				return self.COMPLETED
			if self.is_started():
				if self.is_running():
					return self.RUNNING
				else:
					return self.CRASHED
			if self.is_prepared():
				return self.PREPARED
			return self.NONE
		self.status = check_status_indicators(self)
		return self.status

	def status_str(self):
		return self.status_names[self.find_status()]

	def prepare(self, *args, **kwargs):
		"""
			Prepares the job for execution.

			More steps are likely necessary for child classes.
		"""
		self.status = self.PREPARED
		if not self.is_prepared():
			if self.batch_name:
				mkdirp(join(CALC_DIR, self.batch_name))
			mkdirp(self.directory)
		""" child method add more steps here """

	def _start_pre(self, *args, **kwargs):
		"""
			Some checks at the beginning of .start().
		"""
		if self.is_running() or self.is_complete():
			if not self.queue is None:
				if self.queue.force:
					if self.is_running():
						self.kill()
				else:
					self._log('you are trying to restart a job that is running or completed; ' + \
						'use restart (-e) to skip such jobs or -f to overrule this warning')
					exit()
		if not self.is_prepared():
			self.prepare()

	def _start_post(self, node, pid, *args, **kwargs):
		"""
			Some bookkeeping at the end of .start().
		"""
		self.node = node
		self.pid = pid
		self.save()
		if self.is_running():
			self.STATUS = self.RUNNING
		self._log('starting %s on %s with pid %s' % (self, self.node, self.pid), level = 2)

	def start(self, node, *args, **kwargs):
		"""
			Start the job and store node/pid.
		"""
		self._start_pre(*args, **kwargs)
		"""
			your starting code here
		"""
		self._start_post(node, pid, *args, **kwargs)
		return True

	def fix(self, *args, **kwargs):
		"""
			Some code that can be ran to fix jobs, e.g. after bugfixes or updates.

			Needs to be implemented by children for the specific fix applicable (if just restarting is not viable).
		"""
		return False

	def kill(self, *args, **kwargs):
		"""
			Kills the current job if running using queue methods.

			Any overriding should probably happen in :ref: queue.processes and :ref: queue.stop_job.
		"""
		if self.is_running():
			assert self.node is not None
			assert self.pid is not None
			self._log('killing %s: %s on %s' % (self, self.pid, self.node), level = 2)
			self.queue.stop_job(node = self.node, pid = self.pid)
			return True
		else:
			self._log('job %s not running' % self, level = 2)
			return False

	def cleanup(self, *args, **kwargs):
		if self.is_running() or self.is_complete():
			if not self.queue is None:
				if not self.queue.force:
					self._log('you are trying to clean up a job that is running or completed; ' + \
						'if you are sure you want to do this, use -f (it could also mean that two jobs' + \
					    'are use the same name and batchname).')
					exit()
		if isdir(self.directory):
			rmtree(self.directory, ignore_errors = True)
			return True
		return False

	def result(self, *args, **kwargs):
		"""
			Collects the result of the completed job.

			:return: result of the job; only requirement is that the result be compatible with :ref: summary (and other jobs), but a dict is suggested.
		"""
		if not self.is_complete():
			return None
		return None

	#@classmethod
	#def summary(cls, results, jobs, *args, **kwargs):
	#	"""
	#		Show some sort of summary for all jobs of this class (implementation as you see fit).
	#		:param results: list that contains the result dictionary for any jobs that don't return None;
	#		the ``'job'`` item is set to the applicable job
	#		:param jobs: list of jobs which are of the correct type (this class or group_cls)
	#	"""
	#	#(class method, called once for all jobs)

