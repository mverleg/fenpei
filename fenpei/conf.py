
from functools import partial
from multiprocessing import Pool, cpu_count
from os import environ
from os.path import join, expanduser


if 'CALC_DIR' in environ:
	CALC_DIR = environ['CALC_DIR']
else:
	CALC_DIR = join(expanduser('~'), 'data/sheffield')


def get_pool_light():
	"""
		Process pool for light work, like IO. (This object cannot be serialized so can't be part of Queue).
	"""
	#raise NotImplemented('single process for profiling please')
	if not hasattr(get_pool_light, 'pool'):
		setattr(get_pool_light, 'pool', Pool(min(3 * cpu_count(), 20)))
	return getattr(get_pool_light, 'pool')


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

