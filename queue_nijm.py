
'''
    http://cricket.science.ru.nl/grapher.cgi?target=%2Fclusternodes
'''

from bs4 import BeautifulSoup
from requests import get
from queue import Queue


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
    
    #def add_jobs(self, jobs):
    #    super(DICP_Queue, self).add_jobs(jobs)


