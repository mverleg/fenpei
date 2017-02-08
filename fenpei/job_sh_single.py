
"""
Like ShJob, but with one set of substitutions for all files (or None).

Automatically adds all substitutions as attributes to the job.
"""

from base64 import urlsafe_b64encode
from collections import OrderedDict
from copy import copy
from json import dump, load
from logging import warning
from os import remove
from os.path import join, exists
from struct import pack
from fenpei.job_sh import ShJob, extend_substitutions
from fenpei.shell import run_cmds


class ShJobSingle(ShJob):

	def __init__(self, name, subs, sub_files=(), nosub_files=(), weight=1, batch_name=None,
			defaults_version=1, formatter='jinja2', skip_checks=False, use_symlink=True, force_node=None):
		"""
		Similar to ShJob.

		:param subs: a dictionary of substitutions (not specific to files, contrary to ShJob).
		:param files: files to which substitutions should be applied.
		:param nosub_files: files as-is (no substitutions).
		:param defaults_version: which version of defaults? (Exists to keep old jobs working).
		:param formatter: which formatter to use for files (%, .format, jinja, ..; see `utils.py`).
		"""
		""" Defaults for substitutions. """
		subs_with_defaults = copy(self.get_default_subs(version=defaults_version))
		self.parameter_names = list(subs_with_defaults.keys())
		for sub in subs.keys():
			if sub not in subs_with_defaults:
				warning('job "{0:}" has unknown substitution parameter "{1:s}" = "{2:}"'.format(self, sub, subs[sub]))
		subs_with_defaults.update(subs)
		subs_with_defaults['defaults_version'] = defaults_version
		""" Check/make sure that combiantions of parameters are acceptable """
		checked_subs = self.check_and_update_subs(subs_with_defaults, skip_checks=skip_checks)
		""" Substitutions as job properties. """
		self.substitutions = checked_subs
		for key, val in checked_subs.items():
			setattr(self, key, val)
		""" Override the whole ShJob init because it's very inefficient if all substitutions are the same """
		""" This skips one inheritance level! """
		super(ShJob, self).__init__(name=name, weight=weight, batch_name=batch_name, force_node=force_node)
		self.formatter = formatter
		self.use_symlink = use_symlink
		extend_substitutions(self.substitutions, name, batch_name, self.directory)
		if not hasattr(self.__class__, '_FIXED_CACHE'):
			""" Create the (path, name) -> subst map, but use True instead of the map. """
			files = {filepath: None for filepath in self.get_nosub_files() + list(nosub_files)}
			files.update({filepath: True for filepath in self.get_sub_files() + list(sub_files)})
			self.__class__._FIXED_CACHE = self._fix_files(files)
		""" Now fill in the substitutions (in a copied version). """
		self.files = {fileinfo: (copy(self.substitutions) if subs is True else None) for (fileinfo, subs) in self.__class__._FIXED_CACHE.items()}
		self.parameter_file_path = join(self.directory, 'parameters.json')
	
	def get_param_tuple(self):
		return tuple(self.substitutions[name] for name in self.parameter_names)
	
	def _calc_param_hash(self):
		param_data = hash(tuple(self.substitutions[nm] for nm in self.parameter_names))
		return urlsafe_b64encode(pack('!q', param_data)).rstrip('=')

	@property
	def param_hash(self):
		ShJobSingle._HASH_CACHE = getattr(ShJobSingle, '_HASH_CACHE', {})
		if id(self) not in ShJobSingle._HASH_CACHE:
			ShJobSingle._HASH_CACHE[id(self)] = self._calc_param_hash()
		return ShJobSingle._HASH_CACHE[id(self)]

	def check_and_update_subs(self, subs, *args, **kwargs):
		return subs

	@classmethod
	def get_default_subs(cls, version = 1):
		"""
		:return: default values for substitutions
		"""
		return OrderedDict()

	@classmethod
	def get_files(cls):
		"""
		(used by ShJob; make sure jobs are not added twice)
		"""
		return []

	@classmethod
	def get_sub_files(cls):
		"""
		:return: list of files with substitutions
		"""
		return []

	@classmethod
	def get_nosub_files(cls):
		"""
		:return: list of files without substitutions
		"""
		return []

	def get_input(self):
		subfiles = self.get_sub_files()
		if subfiles:
			return self.files[subfiles[0]]
		return None

	def prepare(self, verbosity=0, *args, **kwargs):
		status = super(ShJobSingle, self).prepare(verbosity=verbosity, *args, **kwargs)
		self.store_config()
		chcmd = 'chmod 750 -R "{0:s}"'.format(self.directory)
		outp = run_cmds((chcmd,), queue=self.queue)
		return status

	def fix(self, verbosity=0, force=False, *args, **kwargs):
		is_fixed = super(ShJobSingle, self).fix(verbosity=verbosity, *args, **kwargs)
		if self.is_prepared() and not exists(self.parameter_file_path):
			self.store_config()
			self._log('stored parameters for {0:}'.format(self), level=2)
			is_fixed = True
		try:
			self.check_config()
		except AssertionError as err:
			if force:
				remove(self.parameter_file_path)
				self.store_config()
				self._log('overwriting stored parameters for {0:} (because of -f)'.format(self), level=2)
				is_fixed = True
			else:
				warning(str(err) + '; use -f to replace the file')
		return is_fixed

	def result(self, verbosity=0, *args, **kwargs):
		self.check_config()
		return super(ShJobSingle, self).result(verbosity=verbosity, *args, **kwargs)

	def crash_reason(self, verbosity=0, *args, **kwargs):
		self.check_config()
		return super(ShJobSingle, self).crash_reason(verbosity=verbosity, *args, **kwargs)

	def store_config(self):
		store = OrderedDict()
		for name in self.parameter_names:
			store[name] = self.substitutions[name]
		with open(self.parameter_file_path, 'w+') as fh:
			dump(obj=store, fp=fh, indent=0)

	def check_config(self):
		if not self.is_prepared():
			return
		try:
			with open(self.parameter_file_path, 'r') as fh:
				retrieved = load(fp=fh)
		except (OSError, IOError) as err:
			raise AssertionError(('Job {0:} has no loadable stored parameters for consistency checking; ' +
				'they can be created using -g [load error: {1:}]').format(self, err))
		problems = []
		before, now = set(retrieved.keys()), set(self.parameter_names)
		if before - now:
			problems.extend('removed parameter {0:s}'.format(s) for s in before - now)
			# 'settings disappeared compared to when job was run: {0:} (-gf to reset this check)'\
			# .format(', '.join(before - now))
		if now - before:
			problems.extend('new parameter {0:s}'.format(s) for s in now - before)
			# 'new settings appeared compared to when job was run: {0:} (-gf to reset this check)'\
			# .format(', '.join(now - before))
		for name in self.parameter_names:
			if name not in self.substitutions or name not in retrieved:
				continue
			if not retrieved[name] == self.substitutions[name]:
				problems.append('changed parameter {0:} from {1:} <{2:}> to {3:} <{4:}>'
					.format(name, type(retrieved[name]).__name__, retrieved[name],
					type(self.substitutions[name]).__name__, self.substitutions[name]))
				# raise AssertionError('parameter {0:} for job {1:} was initially an {2:} <{3:}> but is now {4:} <{5:}>'
				# 	.format(name, self, type(retrieved[name]).__name__, retrieved[name],
				# 	type(self.substitutions[name]).__name__, self.substitutions[name]))
		if problems:
			raise AssertionError(('parameters changed compared to when job was run, which may invalidate the results: '
				'{0:s}; if the results remain valid, reset this using -gf').format(', '.join(problems)))


