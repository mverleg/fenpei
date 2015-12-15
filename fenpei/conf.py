
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
	if not hasattr(get_pool_light, 'pool'):
		setattr(get_pool_light, 'pool', Pool(4 * cpu_count()))
	return getattr(get_pool_light, 'pool')


