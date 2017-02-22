
"""
	Queue using qsub to start jobs.
"""

from os import popen
from os.path import join
from repoze.lru import lru_cache
from fenpei.shell import run_cmds
from fenpei.queue import Queue
from re import findall
from xml.dom.minidom import parse


class QsubQueue(Queue):

	DEFAULT_QSUB_NAME = 'queuename'

	def __init__(self, jobs=None, qname=None, summary_func=None):
		self.qname = qname or self.DEFAULT_QSUB_NAME
		super(QsubQueue, self).__init__(jobs=jobs, summary_func=summary_func)

	def all_nodes(self):
		"""
		Specific nodes are irrelevant; everything in main queue.
		"""
		self._log('no specific nodes; all to general queue', level=2)
		if self.load_nodes():
			return False
		self.nodes = [self.qname]
		self.slots = []
		return True

	def node_availability(self):
		raise NotImplementedError('this should not be implemented for %s because the qsub-queue does the distributing' % self.__class__)

	def distribute_jobs(self, jobs=None, max_reject_spree=None):
		"""
		Let qsub do the distributing by placing everything in general queue.
		"""
		self._log('call to distribute for %d jobs ignored; qsub will do distribution' % len(jobs), level=2)
		self.all_nodes()
		self.distribution = {0: jobs}
		return self.distribution

	@lru_cache(10)
	def _test_qstat(self):
		if run_cmds(['qstat'], queue = self) is None:
			self._log('qstat does not work on this machine; run this code from a node that has access to the queue')
			exit()

	def _get_qstat(self):
		"""
		Get qstat for current user as a dictionary of properties.

		Based on http://stackoverflow.com/questions/26104116/qstat-and-long-job-names
		"""
		f = popen('qstat -xml -r')
		dom = parse(f)
		jobelem = dom.getElementsByTagName('job_info')
		joblist = jobelem[0].getElementsByTagName('job_list')
		jobs = []
		for job in joblist:
			jobstate = job.getElementsByTagName('state')[0].childNodes[0].data
			try:
				qstatqueue = job.getElementsByTagName('queue_name')[0].childNodes[0].data
			except IndexError:
				qstatqueue = '(no queue yet)'
			try:
				node = qstatqueue.split('@')[1].split('.')[0]
			except IndexError:
				node = None
			if 'E' in jobstate:
				jobs.append(None)
			jobs.append({
				'pid': int(job.getElementsByTagName('JB_job_number')[0].childNodes[0].data),
				'name': job.getElementsByTagName('JB_name')[0].childNodes[0].data,
				'user': job.getElementsByTagName('JB_owner')[0].childNodes[0].data,
				'queue': qstatqueue,
				'node': node,
				'state': jobstate,
			})
		return jobs

	@lru_cache(maxsize=100, timeout=2.5)
	def processes(self, node):
		"""
		Get process info from qstat.
		"""
		self._test_qstat()
		self._log('loading processes for %s' % node, level=3)
		return self._get_qstat()

	def stop_job(self, node, pid):
		"""
		Remove individual job from queue.
		"""
		run_cmds(['qdel %s' % pid], queue = self)

	def run_cmd(self, job, cmd):
		"""
		Start an individual job by means of queueing a shell command.
		"""
		self._test_qstat()
		queue = self.qname
		if job.force_node:
			queue = '{0:s}@{1:s}'.format(self.qname, job.force_node)
			self._log('job {0:s} forced queue {1:s}'.format(job, queue), level=2)
		assert job.directory
		subcmd = [
			'qsub',                             # wait in line
				'-b', 'y',                      # it's a binary
				'-cwd',                         # use the current working directory
				'-q', queue,                    # which que to wait in
				'-N', job.name,                 # name of the job
				#'-l slots={0:d}'.format(job.weight), # number of slots = weight of job
					#check this; maybe it's threads rather than processes
				'-e', join(job.directory, 'qsub.err'),  # error directory for the queue
				'-o', join(job.directory, 'qsub.out'),  # output directory for the queue
			'bash -c \'%s\'' % cmd,		    # the actual command (single quotes!)
		]
		cmds = [
			'cd \'%s\'' % job.directory,
			' '.join(subcmd),
		]
		outp = run_cmds(cmds, queue = self)
		self._log(cmds[-1], level=3)
		if not outp or not outp[1]:
			raise self.CmdException('job %s could not be started (output is empty)' % job)
		qid = findall(r'Your job (\d+) \("[^"]+"\) has been submitted', outp[1])[0]
		if not qid:
			raise self.CmdException('job %s id could not be found in "%s"' % (job, outp[1]))
		return int(qid)


