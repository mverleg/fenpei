
"""
	http://cricket.science.ru.nl/grapher.cgi?target=%2Fclusternodes
"""
from datetime import time
from os.path import join

from fenpei.shell import run_cmds_on, run_cmds
from fenpei.queue import Queue
from re import findall


class NijmQueue(Queue):

	QSUB_GENERAL_NAME = 'thchem'

	def all_nodes(self):
		"""
			specific nodes are irrelevant; everything in main queue
		"""
		if not super(NijmQueue, self).all_nodes():
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
		outp = run_cmds(['qstat'], queue = self)
		if not outp:
			return None
		for line in outp.splitlines()[2:]:
			line = line.split()
			if 'E' in line[-6]:
				return None
			return {
				'pid':  int(line[0]),
				'name': ' '.join(line[2:-6]), # in case of space in name somehow
				'user': line[-6],
				'queue': findall(r'@(\w+)\.', line[-2])[0],
			}

	def processes(self, node):
		"""
			get processes on specific node and cache them
		"""
		self._test_qstat()
		if node in self.process_time.keys():
			if time() - self.process_time[node] < 3:
				return self.process_list[node]
		self.log('loading processes for %s' % node, level = 3)
		self.process_time[node] = time()
		self.process_list[node] = []
		ps_dict = self._get_qstat()
		self.process_list[node].append(ps_dict)
		return self.process_list[node]

	def run_cmd(self, job, cmd):
		"""
			start an individual job by means of queueing a shell command
		"""
		self._test_qstat()
		assert job.directory
		cmds = [
			'cd \'%s\'' % job.directory,
			'qsub',                             # wait in line
				'-b', 'y',                      # it's a binary
				'-cwd',                         # use the current working directory
				'-q', self.QSUB_GENERAL_NAME,   # which que to wait in
				'-N', job.name,                 # name of the job
				'-e', join(job.directory, 'qsub.err'),	# error directory for the que
				'-o', join(job.directory, 'qsub.out'),	# output directory for the que
			'"%s"' % cmd,                       # the actual command
		]
		outp = run_cmds_on(cmds, node = job.node, queue = self)[0]
		if not outp:
			raise self.C ('job %s could not be started' % self)
		qid = findall(r'Your job (\d+) ("[^"]+") has been submitted', outp)[0]
		if not qid:
			raise self.C ('job %s id could not be found in "%s"' % (self, outp))
		return qid


