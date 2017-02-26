
"""
Queue using qsub to start jobs.
"""
from distutils.spawn import find_executable
from logging import warning
from os import popen, environ
from os.path import join
from repoze.lru import lru_cache
from fenpei.shell import run_cmds
from fenpei.queue import Queue
from re import findall


class SlurmQueue(Queue):

	def __init__(self, jobs=None, partition=None, summary_func=None):
		self.partition = partition or 'thchem'
		super(SlurmQueue, self).__init__(jobs=jobs, summary_func=summary_func)
		self.time_limit = '07-00:00:00'
		if find_executable('sinfo'):
			partition_info = run_cmds(['sinfo -l --partition {0:s}'.format(self.partition)], queue=self)
			if partition_info:
				partition_lines = partition_info[0].splitlines()[2:]
				if partition_lines:
					time = partition_lines[0].split()[2]
					if findall(r'^[-\d]*\d+:\d+:\d+$', time):
						self.time_limit = time

	def all_nodes(self):
		"""
		Specific nodes are irrelevant; everything in main queue.
		"""
		if not super(SlurmQueue, self).all_nodes():
			return False
		self._log('no specific nodes; all to general queue')
		self.nodes = [self.partition]
		return True

	def node_availability(self):
		raise NotImplementedError('this should not be implemented for %s because the qsub-queue does the distributing' % self.__class__)

	def distribute_jobs(self, jobs=None, max_reject_spree=None):
		"""
		Let slurm do the distributing by placing everything in general queue.
		"""
		self._log('call to distribute for %d jobs ignored; slurm will do distribution' % len(jobs))
		self.all_nodes()
		self.distribution = {0: jobs}
		return self.distribution

	@lru_cache(10)
	def test_slurm(self):
		if not find_executable('sinfo'):
			self._log('slurm does not work on this machine; run this code from a node that has access to the queue')
			exit()

	def get_slurm_stat(self):
		"""
		Get slurm status for current user as a dictionary of properties.
		"""
		with popen('squeue --partition {0:s} --user $USER --format \'%A %B %P %T %100j\''.format(self.partition)) as fh:
			txt = fh.read()
		parts = [line.split(None, 5) for line in txt.splitlines()[1:]]
		jobs = []
		for taskinfo in parts:
			if taskinfo[3] in ('PENDING', 'RUNNING', 'SUSPENDED', 'COMPLETING', 'COMPLETED'):
				jobs.append({
					'pid': int(taskinfo[0]),
					'name': taskinfo[4],
					# 'user': taskinfo[4],
					'queue': taskinfo[2],
					'node': taskinfo[1],
					'state': taskinfo[3],
				})
			else:
				warning(('task with id {0:s} is still in slurm queue, but has state {1:s} ' +
					'(it will be assumed to have crashed)').format(taskinfo[0], taskinfo[3]))
		return jobs

	@lru_cache(maxsize=100, timeout=2.5)
	def processes(self, node):
		"""
		Get process info from qstat.
		"""
		self.test_slurm()
		self._log('loading processes for %s' % node, level=3)
		return self.get_slurm_stat()

	def stop_job(self, node, pid):
		"""
		Remove individual job from queue.
		"""
		run_cmds(['scancel {0:d}'.format(pid)], queue=self)

	def run_cmd(self, job, cmd):
		"""
		Start an individual job by means of queueing a shell command.
		"""
		self.test_slurm()
		assert job.directory
		node_flags = ()
		comment = 'batch: {0:s}; job: {1:s}; weight: {2:d}'.format(job.batch_name, job.name, job.weight)
		if job.force_node:
			node_flags += (
				'--nodelist', str(job.force_node),
				'--no-requeue',
			)
			comment = '{0:s}; forced to node: {1:s}'.format(comment, job.force_node)
		elif 'EXCLUDE_NODES' in environ and environ['EXCLUDE_NODES'].strip():
			node_flags += (
				'--exclude', '"{0:}"'.format(environ['EXCLUDE_NODES']),
			)
			comment = '{0:s}; excl: {1:s}'.format(comment, environ['EXCLUDE_NODES'].strip())
		core_flags = (
			'sbatch',
			'--job-name', '"{0:s}"'.format(job.name),
			'--partition', str(self.partition),
			'--workdir', '"{0:s}"'.format(job.directory),
			'--time', self.time_limit,
			'--mem', '{0:d}G'.format(2+job.weight),
			'--ntasks', '1',  # different tasks can be on different nodes
			'--cpus-per-task', str(max(min(job.weight//1, 10), 1)),
			'--nodes', '1',
			'--output', '"{0:s}"'.format(join(job.directory, 'slurm.all')),
			'--error',  '"{0:s}"'.format(join(job.directory, 'slurm.all')),
		)
		if getattr(job, 'niceness', True):
			node_flags += (
				'--nice={0:}'.format(getattr(job, 'niceness', 100)),  # otherwise other people can't run
			)
			comment = '{0:s}; nice: {1:}'.format(comment, getattr(job, 'niceness', 100))
		core_flags += ('--comment', '"{0:s}"'.format(comment))
		subcmd = ' '.join(core_flags + node_flags + ('\'{0:s}\''.format(cmd),))
		cdcmd = 'cd "{0:s}"'.format(job.directory)
		outp = run_cmds((cdcmd, subcmd,), queue=self)
		self._log(subcmd, level=3)
		if not outp or not outp[1]:
			raise self.CmdException('job {0:s} could not be queued (output is empty)'.format(job))
		qid = findall(r'Submitted batch job (\d+)(\s|$)', outp[1])[0][0]
		if not qid:
			raise self.CmdException('job {0:s} id could not be found in "{1:s}"'.format(job, outp[1]))
		return int(qid)


