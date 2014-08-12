
"""
	like ShJob, but with one set of substitutions for all files (or None)

	automatically adds all substitutions as attributes to the job
"""

from fenpei.job_sh import ShJob


class ShJobSingle(ShJob):

	def __init__(self, name, subs, sub_files = [], nosub_files = [], weight = 1, batch_name = None):
		"""
			similar to ShJob

			:param subs: a dictionary of substitutions (not specific to files, contrary to ShJob)
			:param files: files to which substitutions should be applied
			:param nosub_files: files as-is (no substitutions)
		"""
		for key, val in subs.items():
			setattr(self, key, val)
		substitutions = {filepath: subs for filepath in self.get_sub_files() + sub_files}.update({
						 filepath: None for filepath in self.get_nosub_files() + nosub_files})
		super(ShJobSingle, self).__init__(name = name, substitutions = substitutions, weight = weight, batch_name = batch_name)

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


