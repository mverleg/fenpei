
from functools import partial
from multiprocessing import Pool, cpu_count
from tempfile import gettempdir
from threading import Thread
from bardeen.system import mkdirp
from os import environ, chmod
from os.path import join, expanduser


if 'CALC_DIR' in environ:
	CALC_DIR = environ['CALC_DIR']
else:
	CALC_DIR = join(expanduser('~'), 'data/sheffield')

TMP_DIR = join(gettempdir(), 'fenpei')
mkdirp(TMP_DIR)
chmod(TMP_DIR, 0o700)


def get_pool_light():
	"""
		Process pool for light work, like IO. (This object cannot be serialized so can't be part of Queue). Also consider thread_map.
	"""
	#raise NotImplemented('single process for profiling please')
	if not hasattr(get_pool_light, 'pool'):
		setattr(get_pool_light, 'pool', Pool(min(3 * cpu_count(), 20)))
	return getattr(get_pool_light, 'pool')


def thread_map(func, data):
	"""
		http://code.activestate.com/recipes/577360-a-multithreaded-concurrent-version-of-map/
	"""
	N = len(data)
	result = [None] * N

	# wrapper to dispose the result in the right slot
	def task_wrapper(i):
		result[i] = func(data[i])

	threads = [Thread(target=task_wrapper, args=(i,)) for i in xrange(N)]
	for t in threads:
		t.start()
	for t in threads:
		t.join()

	return result


def _make_inst(params, JobCls):
	return JobCls(**params)


def create_jobs(JobCls, generator, parallel=True):
	"""
		Create jobs from parameters in parallel.
	"""
	if parallel:
		jobs = get_pool_light().map(partial(_make_inst, JobCls=JobCls), tuple(generator))
	else:
		jobs = list(JobCls(**params) for params in generator)
	return jobs


