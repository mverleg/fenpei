
from copy import deepcopy
from numpy import ndarray
from numpy.random import choice, seed as set_seed


def param_sample_random(sample_space, count=100, seed=None):
	"""
	:param sample_space: A dictionary with either
		- Scalar values, which will be used as-is.
		- List, tuple, set or array, which will be chosen from at random (uniform).
		- Scipy distributions, which will be sampled from. [NOT IMPLEMENTED YET]
	:param count: How many samples
	:return: An iterable of samples.
	"""
	# possible improvement: remove duplicates if any
	if seed is not None:
		set_seed(seed)
	sampled = []
	for nr in range(count):
		sample = type(sample_space)()
		for key, value in sample_space.items():
			if isinstance(value, (int, float, str)):
				sample[key] = value
			elif isinstance(value, (list, tuple, set, ndarray)):
				sample[key] = choice(value)
			elif hasattr(value, 'rvs'):
				sample[key] = value.rvs()
			else:
				raise ValueError('param_sample_random does not know what do with object of type {0:}'
					.format(type(value)))
		sampled.append(sample)
	return sampled


def param_sample_grid(sample_space, max_count=10000):
	"""
	:param sample_space: A dictionary with either
		- Scalar values, which will be used as-is.
		- List, tuple, set or array, which will be chosen from at random (uniform).
	:param max_count: Maximum count after which to stop. Happens in chunks so may overshoot.
	:return: An iterable of all samples.
	"""
	def expand(sample, key, only_first=False):
		for val in set(sample.get(key)):
			gen = deepcopy(sample)
			gen[key] = val
			yield gen
			if only_first:
				break
	grid = [sample_space]
	for key, value in sample_space.items():
		# print('>> key', key)
		if isinstance(value, (int, float, str)):
			pass
		elif isinstance(value, (list, tuple, set, ndarray)):
			newgrid = []
			for sample in grid:
				only_first = len(newgrid) >= max_count
				newgrid.extend(expand(sample, key, only_first=only_first))
			grid = newgrid
		elif hasattr(value, 'rvs'):
			raise ValueError('param_sample_grid does not accept probability distributions, try param_sample_random'
				.format(type(value)))
		else:
			raise ValueError('param_sample_grid does not know what do with object of type {0:}'
				.format(type(value)))
	return grid


# def samples_to_code(samples):
# 	"""
# 	UNUSED. Prints the generated samples as Python dictionary definitions (assumes the keys are strings).
# 	"""
# 	for sample in samples:
# 		print('dict({0:s})'.format(
# 			', '.join('{0:s}={1:}'.format(key, '"{0:s}"'.format(value) if isinstance(value, str) else value)
# 				for key, value in sample.items())))


