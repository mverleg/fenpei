
"""
	Distribute jobs over multiple machines by means of ssh.

	- find quiet nodes
	- start jobs if space
	- weight jobs
	- get status info
	- restart failed

"""

from time import time, sleep
from random import sample
from bardeen.collection import group_by
from bardeen.inout import reprint
from datetime import datetime
from collections import defaultdict
from argparse import ArgumentParser
from os import remove
from os.path import basename
from numpy import ceil
from bardeen.mpl import show
from shell import run_cmds_on
from settings import TMP_DIR
from fenpei.job import Job


class Queue(object):

	def __init__(self, jobs = None):
		self.show = 1
		self.force = False
		self.restart = False
		self.all = False
		self.weight = None
		self.limit = None
		self.jobs = []
		self.nodes = []
		self.slots = []
		self.distribution = {}
		self.process_list = {}
		self.process_time = {}
		if jobs:
			self.add_jobs(jobs)

	def _log(self, txt, level = 1):
		"""
			Report to user.
		"""
		if level <= self.show:
			print txt

	def all_nodes(self):
		"""
			Get a list of all nodes (their ssh addresses).
		"""
		if self.load_nodes():
			return False
		self._log('finding nodes')
		self.nodes = []
		self.slots = []
		''' find node ssh adresses and store in self.nodes '''
		return True

	def node_availability(self):
		"""
			Check the processor use of all nodes.
		"""
		if self.load_nodes():
			return False
		if not len(self.nodes):
			self._log('no nodes yet; calling all_nodes()', level = 2)
			self.all_nodes()
			if not len(self.nodes):
				self._log('no nodes found; no availability checked', level = 2)
				return
		self.slots = []
		self._log('checking node availability', level = 1)
		for node in self.nodes:
			outps = run_cmds_on(cmds = ['grep \'model name\' /proc/cpuinfo | wc -l', 'uptime'], node = node, queue = self)
			if len(outps) == 2:
				''' one slot for every 100% processor available '''
				proc_count = int(outps[0])
				load_1min = float(outps[1].split()[-3].replace(',', ''))
				self.slots.append(max(proc_count - load_1min, 0))
				self._log('%2d slots assigned to %6s - 1min cpu %4d%% on %d processors' % (round(self.slots[-1]), self.short_node_name(node), 100 * load_1min, proc_count), level = 2)
			else:
				''' not accessible for some reason '''
				self._log('%s not accessible' % node)
				self.nodes.remove(node)
		self._log('found %d idle processors on %d nodes' % (sum(self.slots), len(self.nodes)))
		self.save_nodes()
		return True

	def save_nodes(self):
		"""
			Save the list of nodes to cache.
		"""
		with open('%s/timestamp.nodes' % TMP_DIR, 'w+') as fh:
			fh.write(str(time()))
		with open('%s/names.nodes' % TMP_DIR, 'w+') as fh:
			fh.write('\n'.join(self.nodes))
		with open('%s/slots.nodes' % TMP_DIR, 'w+') as fh:
			fh.write('\n'.join(['%.4f' % slot for slot in self.slots]))
		self._log('nodes saved')

	def unsave_nodes(self):
		"""
			Remove cached node data.
		"""
		try:
			remove('%s/timestamp.nodes' % TMP_DIR)
			remove('%s/names.nodes' % TMP_DIR)
			remove('%s/slots.nodes' % TMP_DIR)
			self._log('removing stored node info')
		except OSError:
			pass

	def load_nodes(self, memory_time = 10 * 60):
		"""
			Load use restart (-e) to skip such jobs othe list of nodes from cache, if not expired.
		"""
		try:
			with open('%s/timestamp.nodes' % TMP_DIR, 'r') as fh:
				timestamp = float(fh.read())
				dt = time() - timestamp
		except IOError:
			self._log('no stored node info found')
			return False
		if dt < memory_time:
			self._log('loaded node info (age: %ds)' % dt)
		else:
			self._log('stored node info outdated (%ds)' % dt)
			return False
		with open('%s/names.nodes' % TMP_DIR, 'r') as fh:
			self.nodes = fh.read().split()
		with open('%s/slots.nodes' % TMP_DIR, 'r') as fh:
			self.slots = [float(slot) for slot in fh.read().split()]
		return True

	def distribute_jobs(self, jobs = None, max_reject_spree = None):
		"""
			Distribute jobs favourably by means of kind-of-Monte-Carlo (only favourable moves).

			:param jobs: (optional) the jobs to be distributed; uses self.jobs if not provided
			:param max_reject_spree: (optional) stopping criterion; stop when this many unfavourable moves tried in a row
			:return: distribution, a dictionary with node *indixes* as keys and lists of jobs on that node as values
		"""
		if not len(self.slots) > 0:
			self.node_availability()
		if jobs is None:
			jobs = self.jobs
		assert len(self.nodes) == len(self.slots)
		max_reject_spree = 2 * len(self.nodes) if max_reject_spree is None else max_reject_spree
		self._log('distributing %d jobs with weight %d over %d slots' % (len(jobs), self.total_weight(jobs), sum(self.slots)))
		def cost(weight_1, slots_1, weight_2, slots_2):
			return max(weight_1 - slots_1, 0) ** 2 + max(weight_2 - slots_2, 0) ** 2 + slots_1 / max(weight_1, 1) + slots_2 / max(weight_2, 1)
		''' clear the list '''
		distribution = {}
		for node_nr in range(len(self.nodes)):
			distribution[node_nr] = []
		''' random initial job distribution '''
		for job in jobs:
			node_nr = sample(distribution.keys(), 1)[0]
			distribution[node_nr].append(job)
		''' repeat switching until nothing favourable is found anymore '''
		reject_spree, steps = 0, 0
		while reject_spree < 100:
			node1, node2 = sample(distribution.keys(), 2)
			if len(distribution[node1]) > 0:
				steps += 1
				cost_before = cost(self.total_weight(distribution[node1]), self.slots[node1],
								   self.total_weight(distribution[node2]), self.slots[node2])
				item1 = sample(range(len(distribution[node1])), 1)[0]
				cost_switch = cost_move = None
				if len(distribution[node2]) > 0:
					''' compare the cost of switching two items '''
					item2 = sample(range(len(distribution[node2])), 1)[0]
					cost_switch = cost(self.total_weight(distribution[node1]) - distribution[node1][item1].weight + distribution[node2][item2].weight, self.slots[node1],
									   self.total_weight(distribution[node2]) + distribution[node1][item1].weight - distribution[node2][item2].weight, self.slots[node2])
				if cost_before > 0:
					''' compare the cost of moving an item '''
					cost_move = cost(self.total_weight(distribution[node1]) - distribution[node1][item1].weight, self.slots[node1],
									 self.total_weight(distribution[node2]) + distribution[node1][item1].weight, self.slots[node2])
				''' note that None < X for any X, so this works even if only cost_before has an actual value '''
				if (cost_switch < cost_before and cost_switch is not None) or (cost_move < cost_before and cost_move is not None):
					if cost_switch < cost_move and cost_switch is not None:
						''' switch '''
						tmp = distribution[node1][item1]
						distribution[node1][item1] = distribution[node2][item2]
						distribution[node2][item2] = tmp
					elif cost_move is not None:
						''' move (move if equal, it's easier after all) '''
						distribution[node2].append(distribution[node1][item1])
						del distribution[node1][item1]
					reject_spree = 0
				else:
					''' not favorable; don't move '''
					reject_spree += 1
			else:
				''' too many empty slots means few rejectsbut lots of iterations, so in itself a sign to stop '''
				reject_spree += 0.1
		self.distribution = distribution
		''' report results '''
		self._log('distribution found after %d steps' % steps)
		self._log(self.text_distribution(distribution), level = 2)

	def text_distribution(self, distribution):
		"""
			Text visualisation of the distribution of jobs over nodes.
		"""
		lines = []
		no_job_nodes = []
		line_len_guess = max(max(self.total_weight(node_jobs) for node_jobs in distribution.values()), self.slots[0]) + 8
		for node_nr, jobs in distribution.items():
			if len(jobs):
				prog_ind, steps = '', 0
				for job in jobs:
					for k in range(int(round(job.weight - 1))):
						steps += 1
						if steps < self.slots[node_nr]:
							prog_ind += '+'
						else:
							prog_ind += '1'
					steps += 1
					if steps < self.slots[node_nr]:
						prog_ind += 'x'
					else:
						prog_ind += '!'
				prog_ind += '_' * int(round(self.slots[node_nr] - steps))
				prog_ind += ' ' * int(max(line_len_guess - len(prog_ind), 0))
				job_names = ', '.join(str(job) for job in jobs)
				prog_ind += job_names if len(job_names) <= 30 else job_names[:27] + '...'
				lines.append('%5s: %s' % (self.short_node_name(self.nodes[node_nr]), prog_ind))
			else:
				no_job_nodes.append(self.short_node_name(self.nodes[node_nr]))
		if len(no_job_nodes):
			lines.append('no jobs on %d nodes: %s' % (len(no_job_nodes), ', '.join(no_job_nodes)))
		return '\n'.join(lines)

	def short_node_name(self, long_name):
		return long_name

	def total_weight(self, jobs = None):
		"""
			Total weight of the provided jobs, or the added ones if None.
		"""
		if jobs is None:
			jobs = self.jobs
		return sum([job.weight for job in jobs])

	def processes(self, node):
		"""
			Get processes on specific node and cache them.
		"""
		if node in self.process_time.keys():
			if time() - self.process_time[node] < 3:
				return self.process_list[node]
		self._log('loading processes for %s' % node, level = 3)
		self.process_time[node] = time()
		self.process_list[node] = []
		outp = run_cmds_on([
			'ps ux',
		], node = node, queue = self)
		if outp is None:
			self._log('can not connect to %s; are you on the cluster?' % node)
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

	def add_jobs(self, jobs):
		"""
			Add jobs to the queue.
			Recommended to use .job() instead.
		"""

		for job in jobs:
			assert isinstance(job, Job)
			job.queue = self
		self.jobs += jobs
		return self

	def get_jobs(self):
		return self.jobs

	def list_jobs(self):
		N = int(ceil(len(self.jobs) / 3.))
		for k in range(N):
			print '   '.join('%3d. %-25s' % (p + 1, '%s [%d]' % (self.jobs[p].name, self.jobs[p].weight)) for p in [k, k+N, k+2*N] if p < len(self.jobs))

	def run_job(self, job, filepath):
		"""
			Start an individual job, specified by a Python file.
		"""
		cmd = 'nohup python \'%s\' &> out.log &' % basename(filepath)
		return self.run_cmd(job, cmd)

	class CmdException(Exception):
		"""
			An external (e.g. Popen shell script) could not be run.
		"""

	def run_cmd(self, job, cmd):
		"""
			Start an individual job by means of a shell command.

			:param job: the job that's being started this way
			:param cmd: shell commands to run (should include nohup and & as appropriate)
			:return: process id (str)
		"""
		assert job.directory
		cmds = [
			'cd \'%s\'' % job.directory,
			cmd,
			'echo "$\!"' # pid
		]
		outp = run_cmds_on(cmds, node = job.node, queue = self)
		if not outp:
			raise self.CmdException('job %s could not be started' % self)
		return str(int(outp[-1]))

	def stop_job(self, node, pid):
		"""
			Kill an individual job, specified by pid given during start ('pid' could also e.g. be a queue number).
		"""
		run_cmds_on(['kill %s' % pid], node = node, queue = self)

	def prepare(self, *args, **kwargs):
		"""
			Prepare all the currently added jobs (make files etc).
		"""
		prepare_count = 0
		for job in self.jobs:
			prepare_count += int(job.prepare(*args, **kwargs))
		self._log('prepared %d jobs' % prepare_count)

	def running_count(self):
		"""
			How many running jobs.
		"""
		return len(self.get_status()[1][Job.RUNNING])

	def running_weight(self):
		"""
			Total weight of running jobs.
		"""
		return sum(job.weight for job in self.get_status()[1][Job.RUNNING])

	def start(self):
		"""
			Calls corresponding functions depending on flags (e.g. -z, -w, -q, -e).
		"""
		W = None
		if self.all:
			if self.weight:
				self._log('starting all jobs; specific weight (-w) ignored')
			if self.limit:
				self._log('starting all jobs; limit weight (-q) ignored')
			W = float('inf')
		elif self.limit:
			W = max(self.limit - self.running_weight(), 0)
			if not self.weight:
				self._log('starting jobs with weight %d (no minimum)' % W)
			elif W < self.weight:
				self._log('starting jobs with weight %d because of minimum weight %d' % (W, self.weight))
			else:
				self._log('starting jobs with weight %d to fill to %d (higher than minimum)' % (W, self.limit))
		elif self.weight:
			W = self.weight
			self._log('starting jobs with weight %d (by fixed weight)' % self.weight)
		self.start_weight(W)

	def start_weight(self, weight, *args, **kwargs):
		"""
			(Re)start jobs with an approximation of total weight.
		"""
		jobs = self.get_jobs_by_weight(weight)
		if len(jobs):
			self.distribute_jobs(jobs = jobs)
			start_count = 0
			for node_nr, jobs in self.distribution.items():
				for job in jobs:
					job.cleanup(*args, **kwargs)
					start_count += int(job.start(self.nodes[node_nr], *args, **kwargs))
			self._log('(re)started %d jobs' % start_count if self.restart else 'started %d jobs' % start_count)
		else:
			self._log('no jobs to restart' if self.restart else 'no jobs to start')

	def get_jobs_by_weight(self, max_weight):
		"""
			Find jobs with an approximation of total weight.
		"""
		""" find eligible jobs (in specific order) """
		job_status = self.get_status()[1]
		if self.restart:
			startable = job_status[Job.PREPARED] + job_status[Job.NONE] + job_status[Job.CRASHED]
		else:
			startable = job_status[Job.PREPARED] + job_status[Job.NONE]
		if not startable:
			self._log('there are no jobs that can be started')
			return []
		total_weight = sum(job.weight for job in startable)
		if not total_weight:
			""" start only one job """
			jobs = [startable[0]]
		elif max_weight > total_weight or max_weight is None:
			""" start all jobs """
			jobs = startable
		else:
			""" start jobs with specific weight """
			jobs, current_weight = [], 0
			startable = sorted(startable, key = lambda item: - item.weight - (10 if item.status == Job.CRASHED else 0))
			for job in startable:
				if current_weight + job.weight <= max_weight:
					jobs.append(job)
					current_weight += job.weight
		self._log('starting: ' + ', '.join(job.name for job in jobs), level = 2)
		return jobs

	def fix(self, *args, **kwargs):
		"""
			Fix jobs, e.g. after fixes and updates.
		"""
		fix_count = 0
		for job in self.jobs:
			fix_count += int(job.fix(*args, **kwargs))
		self._log('fixed %d jobs' % fix_count)

	def kill(self, *args, **kwargs):
		"""
			Kill all the currently added job processes.
		"""
		kill_count = 0
		for job in self.jobs:
			kill_count += int(job.kill(*args, **kwargs))
		self._log('killed %d jobs' % kill_count)

	def cleanup(self, *args, **kwargs):
		"""
			Clean up all the currently added jobs (remove files).
		"""
		clean_count = 0
		for job in self.jobs:
			if job.cleanup(*args, **kwargs):
				clean_count += 1
		self._log('cleaned up %d jobs' % clean_count)

	def get_status(self):
		"""
			Get list of statusses.
		"""
		status_count = defaultdict(int)
		status_list = defaultdict(list)
		for job in self.jobs:
			job.find_status()
			status_count[job.status] += 1
			status_list[job.status].append(job)
		return status_count, status_list

	def show_status(self, status_count, status_list):
		"""
			Show list of statusses.
		"""

		self._log('status for %d jobs:' % len(self.jobs), level = 1)
		for status_nr in status_list.keys():
			job_names = ', '.join(str(job) for job in status_list[status_nr])
			self._log(' %3d %-12s %s' % (status_count[status_nr], Job.status_names[status_nr], job_names if len(job_names) <= 40 else job_names[:37] + '...'))

	def continuous_status(self, delay = 5):
		"""
			Keep refreshing status until ctrl+C.
		"""

		self._log('monitoring status; use cltr+C to terminate')
		lines = len(Job.status_names) + 1
		print '\n' * lines
		while True:
			try:
				status_count, status_list = self.get_status()

				txt = '%s - job# %d; weight %d:' % (datetime.now().strftime('%H:%M:%S'), self.running_count(), self.running_weight())
				for status_nr in status_list.keys():
					job_names = ', '.join(str(job) for job in status_list[status_nr])
					txt += '\n %3d %-12s %s' % (status_count[status_nr], Job.status_names[status_nr], job_names if len(job_names) <= 40 else job_names[:37] + '...')
				reprint(txt, lines)

				if not status_count[Job.RUNNING]:
					self._log('status monitoring terminated; no more running jobs')
					break

				""" sleep to the next %delay point (e.g. for 5s, check at :05, :10, :15 etc (not :14, :19 etc) """
				sleep(delay - (datetime.now().second + datetime.now().microsecond / 1e6 + .01) % delay)

			except KeyboardInterrupt:
				self._log('status monitoring terminated by user')
				break

	def status(self):
		"""
			Get and show the status of jobs.
		"""
		status_count, status_list = self.get_status()
		self.show_status(status_count, status_list)

	def result(self, *args, **kwargs):
		"""
			:return: a dict of job results, with names as keys
		"""
		results = {}
		for job in self.jobs:
			results[job] = job.result(*args, **kwargs)
		self._log('retrieved results for %d jobs' % len(self.jobs))
		return results

	def summary(self, *args, **kwargs):
		"""
			Summarize the results of all jobs, grouped by type.
		"""
		for cls, jobs in group_by(self.jobs, lambda job: job.group_cls or job.__class__).items():
			self._log('summary for %s' % cls.__name__)
			results = [job.result() for job in jobs]
			cls.summary(results = [result for result in results if result is not None], jobs = jobs, *args, **kwargs)
		show()

#	def _get_argparser(self):
#		"""
#			Return the singleton instance of the ArgumentParser. Useful when overriding run_argv to add more arguments.
#		"""
#		try:
#			self._parser_cache
#		except AttributeError:
#			self._parser_cache = ArgumentParser(description = 'distribute jobs over available nodes', epilog = 'actions are executed (largely) in the order they are supplied; some actions may call others where necessary')
#		return self._parser_cache

	def run_argv(self):
		"""
			Analyze sys.argv and run commands based on it.
		"""
		parser = ArgumentParser(description = 'distribute jobs over available nodes', epilog = 'actions are executed (largely) in the order they are supplied; some actions may call others where necessary')
		parser.add_argument('-v', '--verbose', dest = 'verbosity', action = 'count', default = 0, help = 'more information (can be used multiple times, -vv)')
		parser.add_argument('-f', '--force', dest = 'force', action = 'store_true', help = 'force certain mistake-sensitive steps instead of warning')
		parser.add_argument('-e', '--restart', dest = 'restart', action = 'store_true', help = 'toggle restarting failed jobs')
		parser.add_argument('-a', '--availability', dest = 'availability', action = 'store_true', help = 'list all available nodes and their load (cache reload)')
		parser.add_argument('-d', '--distribute', dest = 'distribute', action = 'store_true', help = 'distribute the jobs over available nodes')
		parser.add_argument('-l', '--list', dest = 'actions', action = 'append_const', const = self.list_jobs, help = 'show a list of added jobs')
		parser.add_argument('-p', '--prepare', dest = 'actions', action = 'append_const', const = self.prepare, help = 'prepare all the jobs')
		parser.add_argument('-c', '--calc', dest = 'actions', action = 'append_const', const = self.start, help = 'start calculating one jobs, or see -z/-w/-q')
		#parser.add_argument('-b', '--keepcalc', dest = 'actions', action = 'append_const', const = None, help = 'like -c, but keeps checking and filling')
		parser.add_argument('-z', '--all', dest = 'all', action = 'store_true', help = '-c will start all jobs')
		parser.add_argument('-w', '--weight', dest = 'weight', action = 'store', type = int, default = None, help = '-c will start jobs with total WEIGHT running')
		parser.add_argument('-q', '--limit', dest = 'limit', action = 'store', type = int, default = None, help = '-c will add jobs until a total LIMIT running')
		parser.add_argument('-k', '--kill', dest = 'actions', action = 'append_const', const = self.kill, help = 'terminate the calculation of all the running jobs')
		parser.add_argument('-r', '--remove', dest = 'actions', action = 'append_const', const = self.cleanup, help = 'clean up all the job files')
		parser.add_argument('-g', '--fix', dest = 'actions', action = 'append_const', const = self.fix, help = 'fix jobs (e.g. after update)')
		parser.add_argument('-s', '--status', dest = 'actions', action = 'append_const', const = self.status, help = 'show job status')
		parser.add_argument('-m', '--monitor', dest = 'actions', action = 'append_const', const = self.continuous_status, help = 'show job status every few seconds')
		parser.add_argument('-x', '--result', dest = 'actions', action = 'append_const', const = self.summary, help = 'collect and show the result of jobs')
		""" Note that some other options may be in use by subclass queues. """
		args = parser.parse_args()

		actions = args.actions or []
		self.show = args.verbosity + 1
		self.force, self.restart, self.all, self.weight, self.limit = \
			args.force, args.restart, args.all, args.weight, args.limit

		if not actions and not any((args.availability, args.distribute, self.restart, self.all, self.weight, self.limit,)):
			self._log('please provide some action')
			parser.print_help()
			return

		if args.availability:
			prev_show, self.show = self.show, 2
			self.unsave_nodes()
			self.all_nodes()
			self.node_availability()
			self.show = prev_show
		if args.distribute:
			self.distribute_jobs()

		if not self.start in actions:
			if self.restart:
				self._log('you requested that failed jobs be restarted, but didn\'t specify a start command [-c]')
				return
			if self.all:
				self._log('you requested that all jobs be started, but didn\'t specify a start command [-c]')
				return
			if self.weight:
				self._log('you specified a weight for jobs to be started, but didn\'t specify a start command [-c]')
				return
			if self.limit:
				self._log('you specified a weight for jobs to keep running, but didn\'t specify a start command [-c]')
				return

		if actions:
			for action in args.actions:
				action()


