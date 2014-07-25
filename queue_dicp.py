
"""
	distribute jobs over multiple machines by means of ssh
	- find quiet nodes
	- start jobs if space
	- weight jobs
	- get status info
	- restart failed
"""

from bs4 import BeautifulSoup
from requests import get
from re import match
from fenpei.queue import Queue


class DICPQueue(Queue):

	def all_nodes(self):
		if not super(DICPQueue, self).all_nodes():
			return
		pattern = 'compute-(?:6|7|8|9)-\d+.local'
		html = get('http://159.226.238.11/ganglia/').text
		soup = BeautifulSoup(html)
		tds = [td for td in soup.find_all('center')[0].find('table').find_all('td') if not td.get('class')]
		for td in tds:
			fnd = match('.*(%s).*' % pattern, td.find('a').get('href'))
			if fnd:
				self.nodes.append(fnd.groups(0)[0])
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


