
from collections import OrderedDict
from functools import partial
from multiprocessing import Pool, cpu_count
from tempfile import gettempdir
#from threading import Thread
from warnings import warn
from bardeen.system import mkdirp
from sys import stderr
from os import environ, chmod
from os.path import join, expanduser
from jinja2 import StrictUndefined


class ParameterValidationError(Exception):
	""" This indicates the parameters for a job failed to validate. """
	def __init__(self, message):
		self.message = message


if 'CALC_DIR' in environ:
	CALC_DIR = environ['CALC_DIR']
else:
	CALC_DIR = join(expanduser('~'), 'data')

TMP_DIR = join(gettempdir(), 'fenpei')
mkdirp(TMP_DIR)
chmod(TMP_DIR, 0o700)


def get_pool_light():
	"""
	Process pool for light work, like IO. (This object cannot be serialized so can't be part of Queue). Also consider thread_map.
	"""
	if not hasattr(get_pool_light, 'pool'):
		setattr(get_pool_light, 'pool', Pool(min(3 * cpu_count(), 20)))
	return getattr(get_pool_light, 'pool')


def thread_map(func, data):
	"""
	http://code.activestate.com/recipes/577360-a-multithreaded-concurrent-version-of-map/
	"""
	#todo: this is disabled
	return map(func, data)

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


def _make_inst(params, JobCls, default_batch=None):
	if 'batch_name' not in params and default_batch:
		params['batch_name'] = default_batch
	try:
		return JobCls(**params)
	except ParameterValidationError as err:
		# note: the job it responsible for logging what went wrong
		# stderr.write('skipping job because it does not validate: {} ; problem: {}\n'.format(params, err.message))
		return None


def create_jobs(JobCls, generator, parallel=True, default_batch=None):
	"""
	Create jobs from parameters in parallel.
	"""
	if parallel:
		try_jobs = get_pool_light().map(partial(_make_inst, JobCls=JobCls, default_batch=default_batch), tuple(generator))
	else:
		try_jobs = list(_make_inst(params, JobCls=JobCls, default_batch=default_batch) for params in generator)
	real_jobs = list(job for job in try_jobs if job is not None)
	if len(real_jobs) < len(try_jobs):
		stderr.write('skipping {} of {} jobs because of validation errors\n'.format(len(try_jobs) - len(real_jobs), len(try_jobs)))
	return real_jobs


def substitute(text, substitutions, formatter, job=None, filename=None):
	"""
	If `formatter` is not callable, this routine is called to choose a built-in formatter.
	"""
	if formatter in [None, 'none']:
		return text
	elif formatter in ['%', 'pypercent']:
		return substitute_pypercent(text, substitutions, job=job, filename=filename)
	elif formatter in ['.format', 'pyformat']:
		return substitute_pyformat(text, substitutions, job=job, filename=filename)
	elif formatter == 'jinja2':
		return substitute_jinja2(text, substitutions, job=job, filename=filename)
	elif formatter == 'jinja':
		warn('Using jinja2 for formatter `jinja`; change to `jinja2` to suppress this warning')
		return substitute_jinja2(text, substitutions, job=job, filename=filename)
	else:
		raise NotImplemented('formatter "{0:}" not known; use a callable or one of `pypercent`, `pyformat` or `jinja2`'.format(formatter))


class FormattingException(Exception): pass


def substitute_pypercent(text, substitutions, job=None, filename=None):
	"""
	Use old Python % formatting to apply substitutions to a string.
	"""
	outp = []
	for nr, line in enumerate(text.splitlines()):
		try:
			outp.append((line % substitutions))
		except KeyError as err:
			raise FormattingException('missing key "{0:s}" in substitution of "{1:s}" on line {2:d}; job not prepared'
				.format(str(err).strip('\''), filename, nr + 1))
		except ValueError:
			raise FormattingException('substitution of "%s" on line %d encountered a formatting error; job not prepared'
				.format(filename, nr + 1))
	return '\n'.join(outp)


def substitute_pyformat(text, substitutions, job=None, filename=None):
	"""
	Use new Python .format() to apply substitutions to a string.
	"""
	outp = []
	for nr, line in enumerate(text.splitlines()):
		try:
			outp.append(line.format(**substitutions))
		except KeyError as err:
			raise FormattingException('missing key "{0:s}" in substitution of "{1:s}" on line {2:d}; job not prepared'
				.format(str(err).strip('\''), filename, nr + 1))
		except ValueError:
			raise FormattingException('substitution of "%s" on line %d encountered a formatting error; job not prepared'
				.format(filename, nr + 1))
	return '\n'.join(outp)


def substitute_jinja2(text, substitutions, job=None, filename=None):
	"""
	Use `jinja2` so apply formatting to a string.
	
	:param text: A string, like the contents of a file, in which substitutions should be applied.
	:param substitutions: Also called 'context', contains a mapping of things to replace.
	:return: Substituted string.
	"""
	try:
		from jinja2 import Template, __version__ as jinja_version
	except ImportError as err:
		raise ImportError('Jinja2 is set as the formatter, but '.format(err))
	if int(jinja_version.split('.')[1]) < 7:
		raise ImportError('Jinja2 needs at least version 2.7, but you have {0:s}'.format(jinja_version))
	from jinja2 import TemplateSyntaxError
	
	try:
		template = Template(text, undefined=StrictUndefined, trim_blocks=True, lstrip_blocks=True)
	except TemplateSyntaxError as err:
		raise TemplateSyntaxError('In file {0:s}: {1:}'.format(filename, err), err.lineno)
	return template.render(**substitutions)


def compare_jobs(jobs, parameters, filter=None):
	"""
	Get a parameters -> job mapping. The parameters are expected to identify unique jobs.

	:param filter: a function that returns True for jobs that should be included.
	:return: Without parameters, a list of jobs. With parameters, a mapping from parameter to accompanying jobs. Indices are tuples of parameter values.
	"""
	if not hasattr(parameters, '__iter__'):
		parameters = (parameters,)
	assert len(parameters) > 0, 'Provide a job attribute to compare jobs.'
	def get_key(jb):
		vals = []
		for param in parameters:
			assert hasattr(jb, param), 'Can not compare jobs on "{0:s}" since job "{1:s}" does not have this attribute.'.format(param, jb)
			vals.append(getattr(jb, param))
		# if len(vals) == 1:
		# 	return vals[0]
		return tuple(vals)
	jobmap = OrderedDict()
	if filter is None:
		filter = lambda obj: True
	for job in jobs:
		if filter(job):
			key = get_key(job)
			assert key not in jobmap, 'Can not compare jobs on "{0:}" since jobs "{1:s}" and "{2:s}" both have value <{3:}>, but values should be unique.'.format(parameters, jobmap[key], job, key)
			jobmap[key] = job
	return jobmap


def compare_results(jobs, parameters, parallel=None, filter=None):
	"""
	Similar to compare_jobs but uses a map from parameters -> results instead. Furthermore, jobs without results are omitted.
	"""
	""" param -> job """
	jobmap = compare_jobs(jobs, parameters, filter=filter)
	""" job -> result """
	results = job_results(jobs=jobmap.values(), parallel=parallel)
	""" param -> result [if complete] """
	return OrderedDict((parval, results[job]) for parval, job in jobmap.items() if results[job] is not None)


def job_results(jobs, parallel=False, *args, **kwargs):
	"""
	:return: a dict of job results, with jobs as keys.
	"""
	if jobs is None:
		jobs = jobs
	results = OrderedDict()
	if parallel:
		resli = get_pool_light().map(job_task('result', **kwargs), jobs)
		for job, res in zip(jobs, resli):
			results[job] = res
	else:
		for job in jobs:
			results[job] = job.result(*args, **kwargs)
	for job, res in results.items():
		if res and 'in' not in res:
			res['in'] = job.get_input()
	return results


def job_task_run(job, method, **kwargs):
	"""
	Runs an arbitrary method of job; used by job_task.
	"""
	return getattr(job, method)(**kwargs)


def job_task(method, **kwargs):
	"""
	Returns a function that runs an arbitrary method of an object, for passing to Pool.map
	"""
	return partial(job_task_run, method=method, **kwargs)


