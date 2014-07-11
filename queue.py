
'''
    distribute jobs over multiple machines by means of ssh
    - find quiet nodes
    - start jobs if space
    - weight jobs
    - get status info
    - restart failed
    - 
'''

from time import time, sleep
from random import sample
from job import Job, status_names, RUNNING
from collections import defaultdict
from argparse import ArgumentParser
from os import remove
from os.path import split
from numpy import ceil
from bardeen.mpl import show
from shell import run_cmds_on
from utility.group_by import group_by
from settings import temp_dir


class Queue(object):
    
    def __init__(self):
        self.show = 1
        self.force = False
        self.jobs = []
        self.nodes = []
        self.slots = []
        self.distribution = {}
        self.process_list = {}
        self.process_time = {}
    
    ''' report to user '''
    def log(self, txt, level = 1):
        if level <= self.show:
            print txt
    
    ''' get a list of all nodes (their ssh addresses) '''
    def all_nodes(self):
        if self.load_nodes():
            return False
        self.log('finding nodes')
        self.nodes = []
        self.slots = []
        ''' find node ssh adresses and store in self.nodes '''
        return True
    
    ''' check the processor use of all nodes '''
    def node_availability(self):
        if self.load_nodes():
            return False
        if not len(self.nodes):
            self.log('no nodes yet; calling all_nodes()', level = 2)
            self.all_nodes()
            if not len(self.nodes):
                self.log('no nodes found; no availability checked', level = 2)
                return
        self.slots = []
        self.log('checking node availability', level = 1)
        for node in self.nodes:
            outps = run_cmds_on(cmds = ['grep \'model name\' /proc/cpuinfo | wc -l', 'uptime'], node = node, queue = self)
            if len(outps) == 2:
                ''' one slot for every 100% processor available '''
                proc_count = int(outps[0])
                load_1min = float(outps[1].split()[-3].replace(',', ''))
                self.slots.append(max(proc_count - load_1min, 0))
                self.log('%2d slots assigned to %6s - 1min cpu %4d%% on %d processors' % (round(self.slots[-1]), self.short_node_name(node), 100 * load_1min, proc_count), level = 2)
            else:
                ''' not accessible for some reason '''
                self.log('%s not accessible' % node)
                self.nodes.remove(node)
        self.log('found %d idle processors on %d nodes' % (sum(self.slots), len(self.nodes)))
        self.save_nodes()
        return True
    
    ''' save the list of nodes to cache '''
    def save_nodes(self):
        with open('%s/timestamp.nodes' % temp_dir, 'w+') as fh:
            fh.write(str(time()))
        with open('%s/names.nodes' % temp_dir, 'w+') as fh:
            fh.write('\n'.join(self.nodes))
        with open('%s/slots.nodes' % temp_dir, 'w+') as fh:
            fh.write('\n'.join(['%.4f' % slot for slot in self.slots]))
        self.log('nodes saved')
    
    ''' remove cached node data '''
    def unsave_nodes(self):
        try:
            remove('%s/timestamp.nodes' % temp_dir)
            remove('%s/names.nodes' % temp_dir)
            remove('%s/slots.nodes' % temp_dir)
            self.log('removing stored node info')
        except OSError:
            pass
    
    ''' load use restart (-e) to skip such jobs othe list of nodes from cache, if not expired '''
    def load_nodes(self, memory_time = 10 * 60):
        try:
            with open('%s/timestamp.nodes' % temp_dir, 'r') as fh:
                timestamp = float(fh.read())
                dt = time() - timestamp
        except IOError:
            self.log('no stored node info found')
            return False
        if dt < memory_time:
            self.log('loaded node info (age: %ds)' % dt)
        else:
            self.log('stored node info outdated (%ds)' % dt)
            return False
        with open('%s/names.nodes' % temp_dir, 'r') as fh:
            self.nodes = fh.read().split()
        with open('%s/slots.nodes' % temp_dir, 'r') as fh:
            self.slots = [float(slot) for slot in fh.read().split()]
        return True
    
    ''' distribute jobs favourably by means of kind-of-Monte-Carlo (only favourable moves) '''
    def distribute_jobs(self, jobs = None, max_reject_spree = None):
        if not len(self.slots) > 0:
            self.node_availability()
        if jobs is None:
            jobs = self.jobs
        assert len(self.nodes) == len(self.slots)
        max_reject_spree = 2 * len(self.nodes) if max_reject_spree is None else max_reject_spree
        self.log('distributing %d jobs with weight %d over %d slots' % (len(jobs), self.total_weight(jobs), sum(self.slots)))
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
        self.log('distribution found after %d steps' % steps)
        self.log(self.text_distribution(distribution), level = 2)
    
    ''' text visualisation of the distribution of jobs over nodes '''
    def text_distribution(self, distribution):
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
    
    ''' get a short version of the node name for display (optional) '''
    def short_node_name(self, long_name):
        return long_name
    
    ''' total weight of the provided jobs, or the added ones if None '''
    def total_weight(self, jobs = None):
        if jobs is None:
            jobs = self.jobs
        return sum([job.weight for job in jobs])
    
    ''' get processes on specific node and cache them '''
    def processes(self, node):
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
    
    ''' add jobs to the queue - recommended to use .job() instead '''
    def add_jobs(self, jobs):
        for job in jobs:
            assert isinstance(job, Job)
            job.queue = self
        self.jobs += jobs
    
    ''' get all registered jobs '''
    def get_jobs(self):
        return self.jobs
    
    ''' print list of added jobs '''
    def list_jobs(self):
        N = int(ceil(len(self.jobs) / 3.))
        for k in range(N):
            print '   '.join('%3d %-25s' % (p + 1, '%s [%d]' % (self.jobs[p].name, self.jobs[p].weight)) for p in [k, k+N, k+2*N] if p < len(self.jobs))
    
    ''' start an individual job, specified by a Python file '''
    def run_job(self, node, filepath):
        directory, filename = split(filepath)
        cmds = [
            'cd \'%s\'' % directory,
            'nohup python \'%s\' &> out.log &' % filename,
            'echo "$\!"'
        ]
        outp = run_cmds_on(cmds, node = node, queue = self)
        if not outp:
            raise Exception('job %s could not be started' % self)
        return str(int(outp[-1]))
    
    ''' kill an individual job, specified by pid given during start ('pid' could also e.g. be a queue number) '''
    def stop_job(self, node, pid):
        run_cmds_on(['kill %s' % pid], node = node, queue = self)
    
    ''' prepare all the currently added jobs (make files etc) '''
    def prepare(self, *args, **kwargs):
        prepare_count = 0
        for job in self.jobs:
            prepare_count += int(job.prepare(*args, **kwargs))
        self.log('prepared %d jobs' % prepare_count)
    
    ''' start all the currently added jobs '''
    def start(self, *args, **kwargs):
        if not len(self.distribution) > 0:
            self.distribute_jobs()
        start_count = 0
        for node_nr, jobs in self.distribution.items():
            for job in jobs:
                start_count += int(job.start(self.nodes[node_nr], *args, **kwargs))
        self.log('started %d jobs' % start_count)
    
    ''' fix jobs, e.g. after fixes and updates '''
    def fix(self, *args, **kwargs):
        fix_count = 0
        for job in self.jobs:
            fix_count += int(job.fix(*args, **kwargs))
        self.log('fixed %d jobs' % fix_count)
    
    ''' restart the jobs aren't running or succesfully completed '''
    def restart(self, *args, **kwargs):
        from job import RUNNING, COMPLETED
        restart_jobs = []
        for job in self.jobs:
            job.find_status()
            if job.status not in [RUNNING, COMPLETED]:
                restart_jobs.append(job)
        if len(restart_jobs):
            if not len(self.distribution) > 0:
                self.distribute_jobs(jobs = restart_jobs)
            start_count = 0
            for node_nr, jobs in self.distribution.items():
                for job in jobs:
                    job.cleanup(*args, **kwargs)
                    start_count += int(job.start(self.nodes[node_nr], *args, **kwargs))
            self.log('restarted %d jobs' % start_count)
        else:
            self.log('no jobs need restarting')
    
    ''' kill all the currently added job processes '''
    def kill(self, *args, **kwargs):
        kill_count = 0
        for job in self.jobs:
            kill_count += int(job.kill(*args, **kwargs))
        self.log('killed %d jobs' % kill_count)
    
    ''' clean up all the currently added jobs (remove files) '''
    def cleanup(self, *args, **kwargs):
        for job in self.jobs:
            job.cleanup(*args, **kwargs)
        self.log('cleaned up %d jobs' % len(self.jobs))
    
    ''' get list of statusses '''
    def get_status(self):
        status_count = defaultdict(int)
        status_list = defaultdict(list)
        for job in self.jobs:
            job.find_status()
            status_count[job.status] += 1
            status_list[job.status].append(job)
        return status_count, status_list
    
    ''' show list of statusses '''
    def show_status(self, status_count, status_list):
        self.log('status for %d jobs:' % len(self.jobs), level = 1)
        for status_nr in status_list.keys():
            job_names = ', '.join(str(job) for job in status_list[status_nr])
            self.log(' %3d %-12s %s' % (status_count[status_nr], status_names[status_nr], job_names if len(job_names) <= 40 else job_names[:37] + '...'))
    
    ''' get and show the status of jobs '''
    def status(self):
        status_count, status_list = self.get_status()
        self.show_status(status_count, status_list)
    
    ''' keep refreshing status until ctrl+C '''
    def continuous_status(self):
        self.log('monitoring status; use cltr+C to terminate')
        while True:
            try:
                status_count, status_list = self.get_status()
                self.show_status(status_count, status_list)
                if not status_count[RUNNING]:
                    self.log('status monitoring terminated; no more running jobs')
                    break
                sleep(5)
            except KeyboardInterrupt:
                self.log('status monitoring terminated by user')
                break
    
    ''' get the results of all jobs (as a dict, with names as keys) '''
    def result(self, *args, **kwargs):
        results = {}
        for job in self.jobs:
            results[job] = job.result(*args, **kwargs)
        self.log('retrieved results for %d jobs' % len(self.jobs))
        return results
    
    ''' summarize the results of all jobs, grouped by type '''
    def summary(self, *args, **kwargs):
        for cls, jobs in group_by(self.jobs, lambda job: job.group_cls or job.__class__).items():
            self.log('summary for %s' % cls.__name__)
            cls.summary(jobs = jobs, *args, **kwargs)
        show()
    
    ''' analyze sys.argv and run commands based on it '''
    def run_argv(self):
        parser = ArgumentParser(description = 'distribute jobs over available nodes', epilog = 'actions are executed (largely) in the order they are supplied; some actions may call others where necessary')
        parser.add_argument('-v', '--verbose', dest = 'verbosity', action = 'count', default = 0, help = 'show more information (can be used multiple times, -vv)')
        parser.add_argument('-f', '--force', dest = 'force', action = 'store_true', help = 'force certain mistake-sensitive steps instead of warning')
        parser.add_argument('-a', '--availability', dest = 'availability', action = 'store_true', help = 'list all available nodes and their load (cache reload)')
        parser.add_argument('-d', '--distribute', dest = 'distribute', action = 'store_true', help = 'distribute the jobs over available nodes')
        parser.add_argument('-l', '--list', dest = 'actions', action = 'append_const', const = self.list_jobs, help = 'show a list of added jobs')
        parser.add_argument('-p', '--prepare', dest = 'actions', action = 'append_const', const = self.prepare, help = 'prepare all the jobs')
        parser.add_argument('-c', '--calc', dest = 'actions', action = 'append_const', const = self.start, help = 'start calculating all the jobs')
        parser.add_argument('-e', '--recalc', dest = 'actions', action = 'append_const', const = self.restart, help = 'restart calculating the jobs that failed')
        parser.add_argument('-k', '--kill', dest = 'actions', action = 'append_const', const = self.kill, help = 'terminate the calculation of all the running jobs')
        parser.add_argument('-r', '--remove', dest = 'actions', action = 'append_const', const = self.cleanup, help = 'clean up all the job files')
        parser.add_argument('-g', '--fix', dest = 'actions', action = 'append_const', const = self.fix, help = 'fix jobs (e.g. after update)')
        parser.add_argument('-s', '--status', dest = 'actions', action = 'append_const', const = self.status, help = 'show job status')
        parser.add_argument('-m', '--monitor', dest = 'actions', action = 'append_const', const = self.continuous_status, help = 'show job status every few seconds')
        parser.add_argument('-x', '--result', dest = 'actions', action = 'append_const', const = self.summary, help = 'collect and show the result of jobs')
        args = parser.parse_args()
        
        if not args.actions and not args.availability and not args.distribute:
            self.log('please provide some action')
            parser.print_help()
            exit()
        
        self.show = args.verbosity + 1
        self.force = args.force
        if args.availability:
            prev_show, self.show = self.show, 2
            self.unsave_nodes()
            self.all_nodes()
            self.node_availability()
            self.show = prev_show
        if args.distribute:
            self.distribute_jobs()
        
        if args.actions:
            for action in args.actions:
                action()


