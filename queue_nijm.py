
'''
    http://cricket.science.ru.nl/grapher.cgi?target=%2Fclusternodes
'''

from bs4 import BeautifulSoup
from requests import get
from fenpei.queue import Queue


class Nijm_Queue(Queue):
    
    def all_nodes(self):
        if not super(Nijm_Queue, self).all_nodes():
            return
        html = get('http://cricket.science.ru.nl/grapher.cgi?target=%2Fclusternodes').text
        soup = BeautifulSoup(html)
        trs = soup.find('table').find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            print tds[1].text.lower()
        exit()
        #self.nodes.append(fnd.groups(0)[0])
        self.nodes = sorted(self.nodes)
        self.nodes = [self.short_node_name(node) for node in self.nodes] 
        self.log('%d nodes found' % len(self.nodes))
        self.log('nodes: %s' % ', '.join(self.nodes), level = 2)
    
    def short_node_name(self, name):
        if 'compute' in name:
            return 'c' + name.replace('compute-', '').replace('.local', '')
        return name
    
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


