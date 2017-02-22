
"""
Base class for fenpei job; this should be considered abstract.

Your custom job(s) should inherit from this job and extend the relevant methods, such as::

* is_prepared
* is_complete
* prepare
* start
* result
* summary
"""

from re import match
from sys import stdout
from bardeen.system import mkdirp
from time import time
from os import remove
from os.path import join, isdir
from shutil import rmtree
from .utils import CALC_DIR


class Job(object):

	CRASHED, NONE, PREPARED, RUNNING, COMPLETED = -1, 0, 1, 2, 3
	status_names = {-1: 'crashed', 0: 'nothing', 1: 'prepared', 2: 'running', 3: 'completed'}

	queue = None
	node = None
	pid = None
	status = None
	""" Set a group_cls to report results together with another class (that has the same output format). """
	group_cls = None

	def __init__(self, name, weight=1, batch_name=None, force_node=None):
		"""
		Create a Job object.

		:param name: unique name consisting of letters, numbers, dot (.) and underscore (_) **YOU need to make sure that name is unique (bijectively maps to job)**
		:param weight: the relative resource use of this job (higher relative weights means fewer jobs will be scheduled together)
		:param batch_name: optionally, a name of the same format as ``name``, which specifies the batch (will be grouped)
		:param force_node: demand a specific node; it's up to the queue whether this is honoured
		"""
		assert match(r'^\w[/\w\.\+_-]*$', name), 'This is not a valid name: "{0:}"'.format(name)
		assert weight > 0
		self.name = name
		self.weight = weight
		self.cluster = None
		self.batch_name = batch_name
		self.force_node = force_node
		if self.batch_name:
			assert match('^\w[\w\._-]*$', batch_name)
			self.directory = join(CALC_DIR, batch_name, name)
		elif batch_name is None:
			raise AssertionError('no batch name for {0:}; provide batch_name argument when creating jobs or set it to False'.format(self))
		elif batch_name is False:
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
				stdout.write('(no queue) ' + txt + '\n')
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
		self._log('job %s saved' % self, level=3)

	def unsave(self):
		"""
		Remove the stored process details.
		"""
		try:
			remove('%s/node_pid.job' % self.directory)
		except IOError:
			pass
		self._log('job %s save file removed' % self.name, level=3)

	def load(self):
		"""
		Load process details from cache.
		"""
		try:
			with open('%s/node_pid.job' % self.directory, 'r') as fh:
				lines = fh.read().splitlines()
				self.node = lines[1]
				self.pid = int(lines[2])
			self._log('job %s loaded' % self.name, level=3)
			return True
		except IOError:
			self._log('job %s save file not found' % self, level=3)
			return False

	def is_prepared(self):
		pass

	def is_started(self):
		if not self.is_prepared():
			return False
		l = self.load()
		return l

	def is_running(self):
		"""
		Only called if at least prepared.
		"""
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
		Check if job completed successfully.

		Needs to be extended by child class.

		Only called for jobs that are at least prepared.
		"""
		return True

	def find_status(self):
		"""
		Find status using is_* methods.
		"""
		def check_status_indicators(self):
			if self.is_prepared():
				if self.is_complete():
					return self.COMPLETED
				elif self.is_started():
					if self.is_running():
						return self.RUNNING
					return self.CRASHED
				return self.PREPARED
			return self.NONE
		if time() - getattr(self, '_last_status_time', time() - 100) > 0.7:
			self.status = check_status_indicators(self)
			setattr(self, '_last_status_time', time())
		return self.status

	def status_str(self):
		return self.status_names[self.find_status()]

	def prepare(self, silent=False, *args, **kwargs):
		"""
		Prepares the job for execution.

		More steps are likely necessary for child classes.
		"""
		self.status = self.PREPARED
		if not self.is_prepared():
			if self.batch_name:
				mkdirp(join(CALC_DIR, self.batch_name))
			mkdirp(self.directory)
		if not silent:
			self._log('preparing {0:s}'.format(self), level=2)
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
					raise AssertionError(('you are trying to restart a job that is running '
						'or completed ({0:} run={1:} complete={2:}); use restart (-e) to '
						'skip such jobs or -f to overrule this warning').format(
						self, self.is_running(), self.is_complete()))
		if not self.is_prepared():
			self.prepare(silent=True)

	def _start_post(self, node, pid, *args, **kwargs):
		"""
		Some bookkeeping at the end of .start().
		"""
		self.node = node
		self.pid = pid
		self.save()
		if self.is_running():
			self.STATUS = self.RUNNING
		self._log('starting %s on %s with pid %s' % (self, self.node, self.pid), level=2)

	def start(self, node, *args, **kwargs):
		"""
		Start the job and store node/pid.
		"""
		self._start_pre(*args, **kwargs)
		"""
		Your starting code here.
		"""
		self._start_post(node, 'pid_here', *args, **kwargs)
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
			self._log('killing %s: %s on %s' % (self, self.pid, self.node), level=2)
			self.queue.stop_job(node = self.node, pid = self.pid)
			return True
		self._log('job %s not running' % self, level=3)
		return False

	def cleanup(self, skip_conflicts=False, *args, **kwargs):
		if self.is_running() or self.is_complete():
			if self.queue is not None and not self.queue.force:
				if skip_conflicts:
					return False
				raise AssertionError(('you are trying to clean up a job ({0:s}; run={1:} complete={2:}) '
					'that is running or completed; use -f to force this, or -e to skip these jobs (it '
					'could also mean that two jobs are use the same name and batchname).').format(
					self.name, self.is_running(), self.is_complete()))
		if self.batch_name is not False and isdir(self.directory):
			rmtree(self.directory, ignore_errors = True)
			self._log('cleaned up {0:s}'.format(self), level=2)
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

	def crash_reason(self, verbosity=0, *args, **kwargs):
		"""
		Find the reason the job has crashed. Should only be called for crashed jobs (by _crash_reason_if_crashed).
		"""
		if verbosity <= 0:
			return '??'
		else:
			return '?? reason for crash not known'

	def _crash_reason_if_crashed(self, verbosity=0, *args, **kwargs):
		if not self.find_status() == self.CRASHED:
			return None
		return self.crash_reason(verbosity=verbosity, *args, **kwargs)


