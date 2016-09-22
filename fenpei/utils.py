
from functools import partial
from multiprocessing import Pool, cpu_count
from tempfile import gettempdir
#from threading import Thread
from warnings import warn
from bardeen.system import mkdirp
from os import environ, chmod
from os.path import join, expanduser

from jinja2 import StrictUndefined

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
	return JobCls(**params)


def create_jobs(JobCls, generator, parallel=True, default_batch=None):
	"""
	Create jobs from parameters in parallel.
	"""
	if parallel:
		jobs = get_pool_light().map(partial(_make_inst, JobCls=JobCls, default_batch=default_batch), tuple(generator))
	else:
		jobs = list(_make_inst(params, JobCls=JobCls, default_batch=default_batch) for params in generator)
	return jobs


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


