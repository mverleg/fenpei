
"""
	Like ShJob, but with one set of substitutions for all files (or None).

	Automatically adds all substitutions as attributes to the job.
"""

from copy import copy
from logging import warning
from fenpei.job_sh import ShJob, extend_substitutions


class ShJobSingle(ShJob):

	def __init__(self, name, subs, sub_files=(), nosub_files=(), weight=1, batch_name=None,
			defaults_version=1, new_format=False, skip_checks=False, use_symlink=True):
		"""
			Similar to ShJob.

			:param subs: a dictionary of substitutions (not specific to files, contrary to ShJob).
			:param files: files to which substitutions should be applied.
			:param nosub_files: files as-is (no substitutions).
			:param defaults_version: which version of defaults? (Exists to keep old jobs working).
			:param new_format: use .format instead of % (python 3 style instead of 2).
		"""
		""" Defaults for substitutions. """
		subs_with_defaults = copy(self.get_default_subs(version = defaults_version))
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
		super(ShJob, self).__init__(name=name, weight=weight, batch_name=batch_name)
		self.use_symlink = use_symlink
		extend_substitutions(self.substitutions, name, batch_name, self.directory)
		if not hasattr(self.__class__, '_FIXED_CACHE'):
			""" Create the (path, name) -> subst map, but use True instead of the map. """
			files = {filepath: None for filepath in self.get_nosub_files() + list(nosub_files)}
			files.update({filepath: True for filepath in self.get_sub_files() + list(sub_files)})
			self.__class__._FIXED_CACHE = self._fix_files(files)
		""" Now fill in the substitutions (in a copied version). """
		self.files = {fileinfo: copy(self.substitutions) if subs is True else None for fileinfo, subs in self.__class__._FIXED_CACHE.items()}

	def check_and_update_subs(self, subs, *args, **kwargs):
		return subs

	@classmethod
	def get_default_subs(cls, version = 1):
		"""
			:return: default values for substitutions
		"""
		return {}

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


