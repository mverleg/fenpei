
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
		self.log('nodes: localhost')
		return True

	def node_availability(self):
		if not self.nodes:
			self.all_nodes()
		self.slots = [10]
		self.log('availability: localhost')
		return True

	def distribute_jobs(self, jobs = None, max_reject_spree = None):
		if not self.slots:
			self.node_availability()
		if jobs is None:
			jobs = self.jobs
		self.distribution = {
			0: jobs
		}
		self.log('distribution: all on localhost')

	def processes(self, node):
		"""
			get processes on specific node and cache them
		"""
		self.log('loading processes for %s' % node, level = 3)
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

	def run_job(self, node, filepath):
		"""
			start an individual job, specified by a Python file
		"""
		directory, filename = split(filepath)
		cmds = [
			'cd \'%s\'' % directory,
			'nohup python \'%s\' &> result.out &' % filename,
			'echo "$\!"'
		]
		outp = run_cmds(cmds, queue = self)
		if not outp:
			raise Exception('job %s could not be started' % self)
		return str(int(outp[-1]))

	def stop_job(self, node, pid):
		"""
			kill an individual job, specified by pid given during start ('pid' could also e.g. be a queue number)
		"""
		run_cmds(['kill %s' % pid], queue = self)


