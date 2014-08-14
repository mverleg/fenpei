
"""
	Queue using qsub to start jobs.
"""

from datetime import time
from os.path import join
from fenpei.shell import run_cmds_on, run_cmds
from fenpei.queue import Queue
from re import findall


class QsubQueue(Queue):

	QSUB_GENERAL_NAME = 'queuename'

	def all_nodes(self):
		"""
			specific nodes are irrelevant; everything in main queue
		"""
		if not super(QsubQueue, self).all_nodes():
			return False
		self._log('no specific nodes; all to general queue')
		self.nodes = [self.QSUB_GENERAL_NAME]
		return True

	def node_availability(self):
		raise NotImplementedError('this should not be implemented for %s because the qsub-queue does the distributing' % self.__class__)

	def distribute_jobs(self, jobs = None, max_reject_spree = None):
		"""
			let qsub do the distributing by placing everything in general queue
		"""
		self._log('call to distribute %d jobs ignored; qsub will do distribution' % len(jobs))
		self.distribution = {0: jobs}

	def _test_qstat(self):
		if run_cmds(['qstat'], queue = self) is None:
			self._log('qstat does not work on this machine; run this code from a node that has access to the queue')
			exit()

	def _get_qstat(self):
		outp = run_cmds(['qstat'], queue = self)[0]
		if not outp:
			yield
		for line in outp.splitlines()[2:]:
			line = line.split()
			print line
			if 'E' in line[-6]:
				yield None
			""" assume no spaces in name """
			qstr = findall(r'@(\w+)\.', line[7])[0] if self.QSUB_GENERAL_NAME in line[7] else None
			yield {
				'pid': int(line[0]),
				'name': ' '.join(line[2]),
				'user': line[3],
				'queue': qstr,
			}

	def processes(self, node):
		"""
			get process info from qstat (no need for caching)
		"""
		self._test_qstat()
		self._log('loading processes for %s' % node, level = 3)
		return [pd for pd in self._get_qstat()]

	def run_cmd(self, job, cmd):
		"""
			start an individual job by means of queueing a shell command
		"""
		self._test_qstat()
		assert job.directory
		subcmd = [
			'qsub',                             # wait in line
				'-b', 'y',                      # it's a binary
				'-cwd',                         # use the current working directory
				'-q', self.QSUB_GENERAL_NAME,   # which que to wait in
				'-N', job.name,                 # name of the job
				'-e', join(job.directory, 'qsub.err'),  # error directory for the que
				'-o', join(job.directory, 'qsub.out'),  # output directory for the que
			'\'%s\'' % cmd,                       # the actual command (single quotes!)
		]
		cmds = [
			'cd \'%s\'' % job.directory,
			' '.join(subcmd),
		]
		outp = run_cmds_on(cmds, node = job.node, queue = self)[1]
		if not outp:
			raise self.CmdException('job %s could not be started' % self)
			
		qid = findall(r'Your job (\d+) \("[^"]+"\) has been submitted', outp)
		if not qid:
			raise self.CmdException('job %s id could not be found in "%s"' % (job, outp))
		return qid[0]


