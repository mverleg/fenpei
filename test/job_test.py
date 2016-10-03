
"""
example test job
needs test/test_run.sh
"""

from os import getcwd
from os.path import join, exists
from re import findall
from fenpei.job_sh_single import ShJobSingle


class TestJob(ShJobSingle):

	@classmethod
	def get_sub_files(cls):
		"""
		:return: list of files with substitutions
		"""
		return [(getcwd(), 'test_run.sh')]

	@classmethod
	def run_file(cls):
		"""
		:return: the path to the file which executes the job; should be in get_files()
		"""
		return 'test_run.sh'

	def get_outp(self):
		outpath = join(self.directory, 'result.txt')
		if exists(outpath):
			join(self.directory, 'result.txt')
			with open(outpath, 'r') as fh:
				return fh.read()
		return ''

	def is_complete(self):
		return super(TestJob, self).is_complete() and 'it is done!' in self.get_outp()

	def result(self, *args, **kwargs):
		outp = self.get_outp()
		if not outp:
			return None
		itemstrs = findall(r'item #(\d+)', outp)
		return {
			'items': [int(item) for item in itemstrs],
			'output': outp,
			'N': self.N,
		}

	@classmethod
	def summary(cls, results, jobs, *args, **kwargs):
		from bardeen.mpl import MPL, subplots
		MPL.xkcd()
		pairs = []
		for di in results:
			x, ys = di['N'], di['items']
			for y in ys:
				pairs.append((x, y))
		if pairs:
			x, y = zip(*pairs)
			fig, ax = subplots(total = 1)
			ax.scatter(x, y)
		else:
			print 'no results yet'


