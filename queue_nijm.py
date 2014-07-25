
"""
	http://cricket.science.ru.nl/grapher.cgi?target=%2Fclusternodes
"""

from bs4 import BeautifulSoup
from requests import get
from fenpei.queue import Queue


class NijmQueue(Queue):

	QSUB_GENERAL_NAME = 'thchem'

	def all_nodes(self):
		if not super(NijmQueue, self).all_nodes():
			return
		html = get('http://cricket.science.ru.nl/grapher.cgi?target=%2Fclusternodes').text
		soup = BeautifulSoup(html)
		trs = soup.find('table').find_all('tr')
		for tr in trs:
			tds = tr.find_all('td')
			print tds[1].text.lower()
		exit()
		# todo: continue
		#self.nodes.append(fnd.groups(0)[0])
		self.nodes = sorted(self.nodes)
		self.nodes = [self.short_node_name(node) for node in self.nodes]
		self.log('%d nodes found' % len(self.nodes))
		self.log('nodes: %s' % ', '.join(self.nodes), level = 2)

	def node_availability(self):
		raise NotImplementedError('this should not be implemented for %s because the qsub-queue does the distributing' % self.__class__)

	def distribute_jobs(self, jobs = None, max_reject_spree = None):
		"""
			let qsub do the distributing by placing everything in general queue
		"""
		self._log('call to distribute %d jobs ignored; qsub will do distribution' % len(jobs))
		self.distribution = {0: jobs}

	def processes(self, node):
		"""
			get processes on specific node and cache them
		"""
		if node in self.process_time.keys():
			if time() - self.process_time[node] < 3:
				return self.process_list[node]
		self.log('loading processes for %s' % node, level = 3)
		self.process_time[node] = time()
		self.process_list[node] = []
		outp = run_cmds_on([
			'ps ux',
		], node = node, queue = self)
		if outp is None:
			self.log('can not connect to %s; are you on the cluster?' % node)
			exit()
		for line in outp[0].splitlines()[1:]:
			cells = line.split()
			ps_dict = {
				'pid':  int(cells[1]),
				'name': ' '.join(cells[10:]),
				'user': cells[0],
				'start':cells[8],
				'time': cells[9],
				'node': node,
			}
			if not ps_dict['name'] == '-bash' and not ps_dict['name'].startswith('sshd: ') and not ps_dict['name'] == 'ps ux':
				self.process_list[node].append(ps_dict)
		return self.process_list[node]

	def run_cmd(self, job, cmd):
		"""
			start an individual job by means of a shell command

			:param job: the job that's being started this way
			:param cmd: shell commands to run (should include nohup and & as appropriate)
			:return: process id (str)
		"""
		# todo: use qsub
		raise NotImplementedError('qsub, if that ever happens')
		assert job.directory
		cmds = [
			'cd \'%s\'' % job.directory,
			cmd,
			'echo "$\!"' # pid
		]
		outp = run_cmds_on(cmds, node = job.node, queue = self)
		if not outp:
			raise self.C ('job %s could not be started' % self)
		return str(int(outp[-1]))


