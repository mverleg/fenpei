
"""
	distribute jobs over multiple machines by means of ssh
	- find quiet nodes
	- start jobs if space
	- weight jobs
	- get status info
	- restart failed
"""

from fenpei.queue import Queue
from fenpei.shell import run_cmds
from os.path import split


class LocalQueue(Queue):

	def all_nodes(self):
		self.nodes = ['localhost']
		self._log('nodes: localhost')
		return True

	def node_availability(self):
		if not self.nodes:
			self.all_nodes()
		self.slots = [10]
		self._log('availability: localhost')
		return True

	def distribute_jobs(self, jobs = None, max_reject_spree = None):
		if not self.slots:
			self.node_availability()
		if jobs is None:
			jobs = self.jobs
		self.distribution = {
			0: jobs
		}
		self._log('distribution: all on localhost')

	def processes(self, node):
		"""
			get processes on specific node and cache them
		"""
		self._log('loading processes for %s' % node, level = 3)
		self.process_list[node] = []
		outp = run_cmds([
			'ps ux',
		], queue = self)
		for line in outp[0].splitlines()[1:]:
			cells = line.split()
			self.process_list[node].append({
				'pid':  int(cells[1]),
				'node': node,
			})
		return self.process_list[node]

	def run_cmd(self, job, cmd):
		"""
			see Queue.run_cmd(), but run everything on local machine
		"""
		assert job.directory
		cmds = [
			'cd \'%s\'' % job.directory,
			cmd,
			'echo "$\!"' # pid
		]
		outp = run_cmds(cmds, queue = self)
		if not outp:
			raise self.CommandException('job %s could not be started' % self)
		return str(int(outp[-1]))

	def stop_job(self, node, pid):
		"""
			kill an individual job, specified by pid given during start ('pid' could also e.g. be a queue number)
		"""
		run_cmds(['kill %s' % pid], queue = self)


