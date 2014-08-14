
"""
	like ShJob, but with one set of substitutions for all files (or None)

	automatically adds all substitutions as attributes to the job
"""

from copy import copy
from fenpei.job_sh import ShJob


class ShJobSingle(ShJob):

	def __init__(self, name, subs, sub_files = [], nosub_files = [], weight = 1, batch_name = None):
		"""
			similar to ShJob

			:param subs: a dictionary of substitutions (not specific to files, contrary to ShJob)
			:param files: files to which substitutions should be applied
			:param nosub_files: files as-is (no substitutions)
		"""
		""" defaults for substitutions """
		subs_with_defaults = copy(self.get_default_subs())
		subs_with_defaults.update(subs)
		""" substitutions as job properties """
		for key, val in subs_with_defaults.items():
			setattr(self, key, val)
		""" convert to per-file format to make a ShJob """
		substitutions = {filepath: subs_with_defaults for filepath in self.get_sub_files() + sub_files}
		substitutions.update({filepath: None for filepath in self.get_nosub_files() + nosub_files})
		super(ShJobSingle, self).__init__(name = name, substitutions = substitutions, weight = weight, batch_name = batch_name)

	@classmethod
	def get_default_subs(cls):
		"""
			:return: default values for substitutions
		"""
		return {}

	@classmethod
	def get_files(cls):
		"""
			(used by ShJob; make sure jobs are nod added twice)
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


