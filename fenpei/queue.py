
"""
Distribute jobs over multiple machines by means of ssh.

- find quiet nodes
- start jobs if space
- weight jobs
- get status info
- restart failed
"""

from argparse import ArgumentParser, SUPPRESS
from collections import defaultdict, OrderedDict
from datetime import datetime
from fnmatch import fnmatch
from functools import partial
from logging import warning
from math import ceil
from os import remove
from os.path import basename, join, exists, isfile
from random import sample
from subprocess import PIPE
from subprocess import Popen
from sys import stdout, stderr
from time import time, sleep
from bardeen.inout import reprint
from fenpei.utils import job_task, compare_jobs, compare_results, job_results
from .job import Job
from .shell import run_cmds_on
from .utils import get_pool_light, TMP_DIR, thread_map


class Queue(object):

	def __init__(self, jobs=None, summary_func=None):
		self.show = 1
		self.force = False
		self.restart = False
		# self.all = False
		self.weight = None
		self.limit = None
		self.jobs = []
		self.nodes = []
		self.slots = []
		self.distribution = {}
		self.process_list = {}
		self.process_time = {}
		self.parallel = None
		if jobs:
			self.add_jobs(jobs)
		if summary_func is not None:
			self.summary = summary_func

	def _log(self, txt, level=1):
		"""
		Report to user.
		"""
		if level <= self.show:
			stdout.write(txt + '\n')

	def all_nodes(self):
		"""
		Get a list of all nodes (their ssh addresses).
		"""
		if self.load_nodes():
			return False
		self._log('finding nodes')
		self.nodes = []
		self.slots = []
		""" find node ssh adresses and store in self.nodes """
		return True

	def node_availability(self):
		"""
		Check the processor use of all nodes.
		"""
		if self.load_nodes():
			return False
		if not len(self.nodes):
			self._log('no nodes yet; calling all_nodes()', level=2)
			self.all_nodes()
			if not len(self.nodes):
				self._log('no nodes found; no availability checked', level=2)
				return
		self.slots = []
		self._log('checking node availability', level=1)
		for node in self.nodes:
			outps = run_cmds_on(cmds = ['grep \'model name\' /proc/cpuinfo | wc -l', 'uptime'], node = node, queue = self)
			if len(outps) == 2:
				""" one slot for every 100% processor available """
				proc_count = int(outps[0])
				load_1min = float(outps[1].split()[-3].replace(',', ''))
				self.slots.append(max(proc_count - load_1min, 0))
				self._log('%2d slots assigned to %6s - 1min cpu %4d%% on %d processors' % (round(self.slots[-1]), self.short_node_name(node), 100 * load_1min, proc_count), level=2)
			else:
				""" not accessible for some reason """
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
			self._log('no stored node info found', level=2)
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
		# max_reject_spree = 2 * len(self.nodes) if max_reject_spree is None else max_reject_spree
		self._log('distributing %d jobs with weight %d over %d slots' % (len(jobs), self.total_weight(jobs), sum(self.slots)))
		def cost(weight_1, slots_1, weight_2, slots_2):
			return max(weight_1 - slots_1, 0) ** 2 + max(weight_2 - slots_2, 0) ** 2 + slots_1 / max(weight_1, 1) + slots_2 / max(weight_2, 1)
		""" clear the list """
		distribution = {}
		for node_nr in range(len(self.nodes)):
			distribution[node_nr] = []
		""" random initial job distribution """
		for job in jobs:
			node_nr = sample(distribution.keys(), 1)[0]
			distribution[node_nr].append(job)
		""" repeat switching until nothing favourable is found anymore """
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
					""" compare the cost of switching two items """
					item2 = sample(range(len(distribution[node2])), 1)[0]
					cost_switch = cost(self.total_weight(distribution[node1]) - distribution[node1][item1].weight + distribution[node2][item2].weight, self.slots[node1],
									   self.total_weight(distribution[node2]) + distribution[node1][item1].weight - distribution[node2][item2].weight, self.slots[node2])
				if cost_before > 0:
					""" compare the cost of moving an item """
					cost_move = cost(self.total_weight(distribution[node1]) - distribution[node1][item1].weight, self.slots[node1],
									 self.total_weight(distribution[node2]) + distribution[node1][item1].weight, self.slots[node2])
				""" note that None < X for any X, so this works even if only cost_before has an actual value """
				if (cost_switch < cost_before and cost_switch is not None) or (cost_move < cost_before and cost_move is not None):
					if cost_switch < cost_move and cost_switch is not None:
						""" switch """
						tmp = distribution[node1][item1]
						distribution[node1][item1] = distribution[node2][item2]
						distribution[node2][item2] = tmp
					elif cost_move is not None:
						""" move (move if equal, it's easier after all) """
						distribution[node2].append(distribution[node1][item1])
						del distribution[node1][item1]
					reject_spree = 0
				else:
					""" not favorable; don't move """
					reject_spree += 1
			else:
				""" too many empty slots means few rejectsbut lots of iterations, so in itself a sign to stop """
				reject_spree += 0.1
		self.distribution = distribution
		""" report results """
		self._log('distribution found after {0:d} steps'.format(steps))
		self._log(self.text_distribution(distribution), level=2)
		return self.distribution

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
		self._log('loading processes for %s' % node, level=3)
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

	def add_job(self, job):
		"""
		Add single job to the queue.
		"""
		assert isinstance(job, Job)
		job.queue = self
		self.jobs.append(job)
		return self

	def add_jobs(self, jobs):
		"""
		Add list of jobs to the queue.
		"""
		for job in jobs:
			self.add_job(job)
		self._same_path_check()
		return self

	def get_jobs(self):
		return self.jobs

	def compare_jobs(self, parameters, filter=None):
		return compare_jobs(self.jobs, parameters, filter=filter)
	
	def compare_results(self, parameters, filter=None):
		return compare_results(self.jobs, parameters, filter=filter)
	
	def result(self, parallel=None, *args, **kwargs):
		parallel = self.parallel if parallel is None else parallel
		results = job_results(parallel=parallel, *args, **kwargs)
		self._log('retrieved results for %d jobs' % len(self.jobs))
		return results

	def list_jobs(self, cols=2, verbosity=0, *args, **kwargs):
		N = int(ceil(len(self.jobs) / float(cols)))
		for k in range(N):
			stdout.write('  | '.join(
				'{0:2d}. {1:20s} {2:>10s}'.format(p + 1, '{0:s} [{1:d}]'.format(
					join(self.jobs[p].batch_name, self.jobs[p].name) if verbosity else self.jobs[p].name,
					self.jobs[p].weight
				), self.jobs[p].status_str())
					for p in [k, k+N, k+2*N] if p < len(self.jobs)
			) + '\n')

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

	def prepare(self, parallel=None, *args, **kwargs):
		"""
		Prepare all the currently added jobs (make files etc).
		"""
		parallel = self.parallel if parallel is None else parallel
		if parallel:
			statuses = thread_map(job_task('prepare', **kwargs), self.jobs)
		else:
			statuses = (job.prepare(**kwargs) for job in self.jobs)
		prepare_count = sum(int(status) for status in statuses)
		self._log('prepared %d jobs' % prepare_count)

	def start(self, parallel=None, verbosity=0, *args, **kwargs):
		"""
		Calls corresponding functions depending on flags (e.g. -z, -w, -q, -e).
		"""
		self._quota_warning()
		self._same_path_check(fail=True)
		job_status = self.get_status()
		start_jobs = self.select_start_jobs(weight=self.weight, limit=self.limit,
			restart=self.restart, job_status=job_status)
		self._log('starting {0:d} jobs with weight {1:d}'.format(
			len(start_jobs), sum((job.weight for job in start_jobs), 0)), level=2)
		distribution = self.distribute_jobs(jobs=start_jobs)
		start_cnt = 0
		for node_nr, jobs in distribution.items():
			""" Removed parallism here because it made things slower (and more complicated). """
			for job in jobs:
				node = self.nodes[node_nr]
				if job.is_started():
					job.cleanup(**kwargs)
				start_cnt += job.start(node, **kwargs)
		self._log('started {0:d} jobs'.format(start_cnt, level=1))

	def select_start_jobs(self, weight, limit, restart, job_status=None):
		"""
		Find jobs in job_status or self.get_status() that can be started to stay within the weight and count limits provided.
		
		:param weight: Total running weight limit.
		:param limit: Total running count limit.
		:param restart: Whether crashed jobs should be restarted (at comparable weight, start fresh ones first).
		:return:
		"""
		""" Find jobs with startable status. """
		if job_status is None:
			job_status = self.get_status()
		startable = job_status[Job.PREPARED] + job_status[Job.NONE]
		for job in startable:
			job._crash_score = 1
		if restart:
			self._log('crashed jobs are eligible for restarting', level=2)
			for job in job_status[Job.CRASHED]:
				job._crash_score = 0
			startable.extend(job_status[Job.CRASHED])
		else:
			self._log('only unstarted jobs will be started', level=2)
		if not self.weight and not self.limit:
			return startable
		startable = sorted(startable, key=lambda job: job.weight + 2 * job._crash_score, reverse=False)
		""" Remove jobs to reach the requested weight. """
		if weight is not None:
			start_jobs = []
			running_weight = sum(job.weight for job in job_status[Job.RUNNING])
			self._log('{0:d} jobs with weight {1:d} already running'.format(len(job_status[Job.RUNNING]), running_weight), level=2)
			weight_left = weight - running_weight
			while startable and weight_left > 0:
				consider_job = startable.pop()
				if weight_left > consider_job.weight:
					weight_left -= consider_job.weight
					start_jobs.append(consider_job)
			self._log('pre-selecting {0:d} jobs with weight {1:d} to stay under the weight limit of {2:d}'
				.format(len(start_jobs), sum((job.weight for job in start_jobs), 0), weight), level=2)
		else:
			start_jobs = startable
			self._log('pre-selecting all {0:d} eligible jobs for starting'.format(len(start_jobs)), level=2)
		""" Limit the total number of jobs to start. """
		if limit is not None:
			running_count = len(job_status[Job.RUNNING])
			start_count = max(min(limit - running_count, len(start_jobs)), 0)
			self._log(('starting {0:d} of {1:d} eligible jobs, to stay under the limit of {2:d} '
				'jobs, with {3:d} already running').format(start_count, len(start_jobs), limit,
				running_count), level=2)
			start_jobs = start_jobs[-start_count:]
			if start_count == 0:
				start_jobs = []
			assert len(start_jobs) <= start_count, 'too many jobs: {0:d} <= {1:d}'.format(len(start_jobs), start_count)
		else:
			self.log('keeping {0:d} for starting since no limit was provided'.format(len(start_jobs)), level=2)
		return start_jobs

	def _quota_warning(self):
		try:
			out, err = Popen(['quota', '-Q'], stdout=PIPE, stderr=PIPE).communicate()
		except OSError:
			""" No quota """
		else:
			parts = out.splitlines()[-1].split()
			used, avail = float(parts[0].strip('*')), float(parts[1].strip('*'))
			if avail - used < 1e7 and used / avail > 0.8:
				warning('there is only {0:d}MB free ({1:d}%)'.format(
					int((avail - used) / 1024), int(100 * (1 - float(used) / avail))))

	def _same_path_check(self, fail=False):
		pths = set()
		found = 0
		for job in self.jobs:
			if (job.batch_name, job.name) in pths:
				if job.batch_name:
					pthname = '{0:s}/{1:s}'.format(job.batch_name, job.name)
				else:
					pthname = job.name
				msg = 'there are multiple jobs with location {0:s}'.format(pthname)
				if fail:
					raise AssertionError(msg)
				found += 1
				if found >= 3:
					warning(msg + '. Stopping duplicate checks now.')
					break
				warning(msg)
			pths.add((job.batch_name, job.name))
		return pths

	def fix(self, parallel=None, *args, **kwargs):
		"""
		Fix jobs, e.g. after fixes and updates.
		"""
		parallel = self.parallel if parallel is None else parallel
		if parallel:
			statuses = thread_map(job_task('fix', **kwargs), self.jobs)
		else:
			statuses = (job.fix(**kwargs) for job in self.jobs)
		fix_count = sum(int(status) for status in statuses)
		self._log('fixed %d jobs' % fix_count)

	def kill(self, *args, **kwargs):
		"""
		Kill all the currently added job processes.
		"""
		kill_count = 0
		for job in self.jobs:
			kill_count += int(job.kill(*args, **kwargs))
		self._log('killed %d jobs' % kill_count)

	def cleanup(self, parallel=None, *args, **kwargs):
		"""
		Clean up all the currently added jobs (remove files).
		"""
		parallel = self.parallel if parallel is None else parallel
		if parallel:
			#statuses = get_pool_light().map(job_task('cleanup', **kwargs), self.jobs)
			statuses = thread_map(job_task('cleanup', skip_conflicts=self.restart, **kwargs), self.jobs)
		else:
			statuses = (job.cleanup(skip_conflicts=self.restart, **kwargs) for job in self.jobs)
		cleanup_count = sum(int(status) for status in statuses)
		self._log('cleaned up %d jobs' % cleanup_count)

	def get_status(self, parallel=None, **kwargs):
		"""
		Get list of statusses.
		"""
		parallel = self.parallel if parallel is None else parallel
		if parallel:
			status_res = thread_map(job_task('find_status', **kwargs), self.jobs)
			statuses = dict(zip(self.jobs, status_res))
		else:
			statuses = {}
			for job in self.jobs:
				statuses[job] = job.find_status(**kwargs)
		# status_count = defaultdict(int)
		status_list = defaultdict(list)
		for job, status in statuses.items():
			# status_count[status] += 1
			status_list[status].append(job)
		return status_list

	def show_status(self, status_list, verbosity=0):
		"""
		Show list of statusses.
		"""
		self._log('status for %d jobs:' % len(self.jobs), level=1)
		for status_nr in status_list.keys():
			if verbosity <= 0:
				job_names = ' '.join(str(job) for job in status_list[status_nr][:20 ])
				job_names = job_names if len(job_names) <= 40 else job_names[:37] + '...'
				self._log(' {0:3d} {1:12s} {2:s}'.format(
					len(status_list[status_nr]), Job.status_names[status_nr], job_names))
			else:
				job_names = ' '.join(str(job) for job in status_list[status_nr])
				weight = sum(job.weight for job in self.get_status()[status_nr])
				stat_wght = '{1:s} ({0:d})'.format(weight, Job.status_names[status_nr])
				self._log(' {0:3d} {2:18s} {1:s}'.format(
					len(status_list[status_nr]), job_names, stat_wght))

	def continuous_status(self, delay=5, *args, **kwargs):
		"""
		Keep refreshing status until ctrl+C.
		"""
		self._log('monitoring status; use cltr+C to terminate')
		lines = len(Job.status_names) + 1
		stdout.write('\n' * lines + '\n')
		nothing_running_count = 0
		while True:
			try:
				status_list = self.get_status()
				running_count = len(status_list[Job.RUNNING])
				running_weight = sum((job.weight for job in status_list[Job.RUNNING]), 0)
				txt = '{0:s} - job# {1:d}; weight {2:d}:'.format(datetime.now().strftime('%H:%M:%S'),
					running_count, running_weight)
				for status_nr in status_list.keys():
					job_names = ', '.join(str(job) for job in status_list[status_nr])
					txt += '\n %3d %-12s %s' % (len(status_list[status_nr]), Job.status_names[status_nr], job_names if len(job_names) <= 40 else job_names[:37] + '...')
				reprint(txt, lines)
				if not status_list[Job.RUNNING]:
					nothing_running_count += 1
				else:
					nothing_running_count = 0

				if nothing_running_count >= 3:
					self._log('status monitoring terminated; no more running jobs')
					break

				""" sleep to the next %delay point (e.g. for 5s, check at :05, :10, :15 etc (not :14, :19 etc) """
				sleep(delay - (datetime.now().second + datetime.now().microsecond / 1e6 + .01) % delay)

			except KeyboardInterrupt:
				self._log('status monitoring terminated by user')
				break

	def status(self, verbosity=0, *args, **kwargs):
		"""
		Get and show the status of jobs.
		"""
		status_list = self.get_status()
		self.show_status(status_list, verbosity=verbosity)

	# @staticmethod
	# def pre_summary(queue, *args, **kwargs):
	# 	pass
	
	@staticmethod
	def summary(queue):
		raise NotImplementedError(('No summary function (queue "{0:}"). Attach a '
			'static method .summary(queue) to the queue.').format(queue))

	# @staticmethod
	# def post_summary(queue, *args, **kwargs):
	# 	pass

	def get_crash_reason(self, parallel=None, verbosity=0):
		"""
		For each failed job, print why it failed.
		"""
		parallel = self.parallel if parallel is None else parallel
		reasons = OrderedDict()
		if parallel:
			#resli = get_pool_light().map(job_task('_crash_reason_if_crashed', verbosity=verbosity), self.jobs)
			#resli = thread_map(job_task('_crash_reason_if_crashed', verbosity=verbosity), self.jobs)
			resli = [job._crash_reason_if_crashed(verbosity=verbosity) for job in self.jobs]
		else:
			#resli = thread_map(job_task('_crash_reason_if_crashed', verbosity=verbosity), self.jobs)
			resli = [job._crash_reason_if_crashed(verbosity=verbosity) for job in self.jobs]
		for job, res in zip(self.jobs, resli):
			if res is not None:
				reasons[job] = res
		self._log('retrieved crash reasons for %d jobs' % len(reasons))
		return reasons

	def crash_reason(self, parallel=None, verbosity=0, line_len=70, *args, **kwargs):
		reasons = self.get_crash_reason(parallel=parallel, verbosity=verbosity)
		if not reasons: return
		max_name_len = max((len(job.batch_name) + len(job.name)) for job in reasons.keys()) + 1
		log_template = '{0:' + str(max_name_len) + 's} {1:s}\n'
		for job, reason in reasons.items():
			txt = [reason[k:k+line_len] for k in range(0, len(reason), line_len)]
			for k, line in enumerate(txt):
				jobname = '{0:}/{1:}'.format(job.batch_name, job.name) if k==0 else ''
				stdout.write(log_template.format(jobname, line))

	def run_cmd_per_job(self, cmd, parallel=None, verbosity=0, status=None, *args, **kwargs):
		"""
		Run a shell command in each job's directory (for --cmd).
		"""
		if status is None:
			jobs = self.jobs
		else:
			jobs = self.get_status()[status]
		cmd_count = 0
		jobs = sorted(jobs, key=lambda job: job.name)
		for job in jobs:
			if job.is_prepared():
				self._log('running command for {0:s}'.format(job.name), level=2)
				scmd = 'cd \'{dir:s}\'; bash -c \'export NAME=\'"\'"\'{name:s}\'"\'"\' BATCH=\'"\'"\'{batch:s}\'"\'"\' STATUS=\'"\'"\'{status:s}\'"\'"\'; {cmd:s}\''.format(
					dir=job.directory, name=job.name, batch=job.batch_name,
					status=job.status_names[job.status], cmd=cmd
				)
				self._log(scmd, level=3)
				process = Popen([scmd], shell=True)
				process.communicate()
				cmd_count += 1
		self._log('ran command `{1:s}` for {0:d} jobs'.format(cmd_count, cmd), level=1)
	
	def filter_jobs(self, arg, jobs):
		"""
		Filter jobs by pattern or file, for --jobs argument.
		"""
		keep_jobs = []
		if '/' in arg or '\\' in arg:
			self._log('jobs argument "{0:s}" interpreted as file since it contains / or \\'.format(arg), level=3)
			assert isfile(arg), '--jobs argument seems to be a file path, but the file does not exist'
			with open(arg, 'r+') as fh:
				requested = set(pth.strip() for pth in fh.read().splitlines())
				if '' in requested: requested.remove('')
				self._log('filtering jobs by {0:d} names from "{1:s}"'.format(len(requested), arg), level=2)
				for job in jobs:
					if job.name in requested:
						keep_jobs.append(job)
						requested.remove(job.name)
				# self.jobs = keep
				unmatched = requested
		else:
			parts = arg.split()
			self._log('{0:d} jobs arguments interpreted as patterns since they do not contains / or \\'
				.format(len(parts)), level=3)
			requested = dict((ptrn, 0) for ptrn in parts)
			self._log('filtering jobs by {0:d} patterns'.format(len(requested)), level=2)
			if len(jobs) * len(requested) > 500:
				warning('many jobs and/or many --jobs filters; this may take a while')
			for job in tuple(jobs):
				do_keep = False
				for ptrn in requested.keys():
					if fnmatch(job.name, ptrn):
						requested[ptrn] += 1
						do_keep = True
				if do_keep:
					keep_jobs.append(job)
			unmatched = tuple(ptrn for ptrn, cnt in requested.items() if not cnt)
		if unmatched:
			raise ValueError('Specifically requested job(s) [{0:s}] was/were not found.'
				.format(', '.join(unmatched)))
		return keep_jobs
	
	def run_argv(self):
		"""
		Analyze sys.argv and run commands based on it.
		"""
		def wrap_summary(queue=self, *args, **kwargs):
			# self.pre_summary(queue, *args, **kwargs)
			output = self.summary(queue)
			# self.post_summary(queue, *args, **kwargs)
			return output
		def wrap_cmd(cmd): return partial(self.run_cmd_per_job, cmd=cmd)
		def wrap_prep_cmd(cmd): return partial(self.run_cmd_per_job, cmd=cmd, status=Job.PREPARED)
		def wrap_run_cmd(cmd): return partial(self.run_cmd_per_job, cmd=cmd, status=Job.RUNNING)
		def wrap_comp_cmd(cmd): return partial(self.run_cmd_per_job, cmd=cmd, status=Job.COMPLETED)
		def wrap_crash_cmd(cmd): return partial(self.run_cmd_per_job, cmd=cmd, status=Job.CRASHED)
		parser = ArgumentParser(description='distribute jobs over available nodes',
			epilog='actions are executed (largely) in the order they are supplied; some actions may call others where necessary')
		parser.add_argument('-v', '--verbose', dest='verbosity', action='count', default=0, help='more information (can be used multiple times, -vv)')
		parser.add_argument('-f', '--force', dest='force', action='store_true', help='force certain mistake-sensitive steps instead of failing with a warning')
		parser.add_argument('-e', '--restart', dest='restart', action='store_true', help='with this, start and cleanup ignore complete (/running) jobs')
		parser.add_argument('-a', '--availability', dest='availability', action='store_true', help='list all available nodes and their load (cache reload)')
		parser.add_argument('-d', '--distribute', dest='distribute', action='store_true', help='distribute the jobs over available nodes')
		parser.add_argument('-l', '--list', dest='actions', action='append_const', const=self.list_jobs, help='show a list of added jobs')
		parser.add_argument('-p', '--prepare', dest='actions', action='append_const', const=self.prepare, help='prepare all the jobs')
		parser.add_argument('-c', '--calc', dest='actions', action='append_const', const=self.start, help='start calculating one jobs, or see -z/-w/-q')
		#parser.add_argument('-b', '--keepcalc', dest='actions', action='append_const', const=None, help='like -c, but keeps checking and filling')
		# parser.add_argument('-z', '--all', dest='all', action='store_true', help='-c will start all jobs')
		parser.add_argument('-w', '--weight', dest='weight', action='store', type=int, default=None, help='-c will start jobs with total weight is running')
		parser.add_argument('-q', '--limit', dest='limit', action='store', type=int, default=None, help='-c will add jobs until a total amount is running')
		parser.add_argument('-k', '--kill', dest='actions', action='append_const', const=self.kill, help='terminate the calculation of all the running jobs')
		parser.add_argument('-r', '--remove', dest='actions', action='append_const', const=self.cleanup, help='clean up all the job files')
		parser.add_argument('-g', '--fix', dest='actions', action='append_const', const=self.fix, help='fix jobs, check cache etc (e.g. after update)')
		parser.add_argument('-s', '--status', dest='actions', action='append_const', const=self.status, help='show job status')
		parser.add_argument('-m', '--monitor', dest='actions', action='append_const', const=self.continuous_status, help='show job status every few seconds')
		parser.add_argument('-x', '--result', dest='actions', action='append_const', const=wrap_summary, help='run analysis code to summarize results')
		parser.add_argument('-t', '--whyfail', dest='actions', action='append_const', const=self.crash_reason, help ='print a list of failed jobs with the reason why they failed')
		parser.add_argument('-j', '--serial', dest='parallel', action='store_false', help='make job commands (start, fix, etc) serial (parallel is faster but order is inconsistent)')
		parser.add_argument('--jobs', dest='jobs', action='store', type=str, help=('if argument contains \ or /, it should be a file containing a job name per line; '
			'otherwise it is a string specifying by name the jobs to (re)start or use, separated by whitespace, ?*[] patterns allowed'))
		# parser.add_argument('--cmd', dest='cmd', nargs=1, action='store', type=str, help='run shell command in each of the job directories')
		parser.add_argument('--cmd', dest='actions', nargs=1, action='append', type=wrap_cmd, help='run a shell command in the directories of each job that has a dir ' + \
			'($NAME/$BATCH/$STATUS if -s); also prep-cmd, run-cmd, comp-cmd, crash-cmd')
		parser.add_argument('--prep-cmd', dest='actions', nargs=1, action='append', type=wrap_prep_cmd, help=SUPPRESS)
		parser.add_argument('--run-cmd', dest='actions', nargs=1, action='append', type=wrap_run_cmd, help=SUPPRESS)
		parser.add_argument('--comp-cmd', dest='actions', nargs=1, action='append', type=wrap_comp_cmd, help=SUPPRESS)
		parser.add_argument('--crash-cmd', dest='actions', nargs=1, action='append', type=wrap_crash_cmd, help=SUPPRESS)
		# remaining letters: bjnu  [-i, -y and -o are available but have commmon meanings]
		""" Note that some other options may be in use by subclass queues. """
		args = parser.parse_args()

		self.show = args.verbosity + 1
		self.force, self.restart, self.weight, self.limit = \
			args.force, args.restart, args.weight, args.limit

		""" Handle only specific jobs by deleting the others (not the most clean perhaps, but good enough for the occasional use). """
		if args.jobs:
			try:
				self.jobs = self.filter_jobs(args.jobs, self.jobs)
			except ValueError as err:
				parser.error(str(err))

		if not args.actions:
			stderr.write('No action selected. Use one or several flags to control actions.\n\n')
			parser.print_help()
			exit()

		actions = []
		for act in args.actions:
			if hasattr(act, '__iter__'):
				actions.extend(act)
			else:
				actions.append(act)

		if not actions and not any((args.availability, args.distribute, self.restart, self.weight, self.limit,)):
			parser.print_help()
			parser.error('Please provide some action.')

		if args.availability:
			prev_show, self.show = self.show, 2
			self.unsave_nodes()
			self.all_nodes()
			self.node_availability()
			self.show = prev_show
		if args.distribute:
			self.distribute_jobs()

		if self.restart:
			if self.start not in actions and self.cleanup not in actions:
				parser.error('You requested that restart/cleanup apply only to failed jobs, '
					'but didn\'t specify a start or cleanup command [-c].')

		if self.start not in actions:
			if self.weight:
				parser.error('You specified a weight for jobs to be started, '
					'but didn\'t specify a start command [-c].')
			if self.limit:
				parser.error('You specified a weight for jobs to keep running, '
					'but didn\'t specify a start command [-c].')

		self.parallel = args.parallel
		if actions:
			for action in actions:
				action(verbosity=args.verbosity, parallel=args.parallel, force=args.force)

		return [str(action) for action in actions]


# def _start_job_on_node(info, **kwargs):
# 	job, node = info
	# job.cleanup(**kwargs)
	# return job.start(node, **kwargs)


