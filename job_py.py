
"""
	extended base class for fenpei job that runs through a Python file; this should be considered abstract

	as with :ref:Job, your custom job(s) should inherit from this job and extend the relevant methods.
	Instead of :ref: prepare and :ref: start, you can override:

	* run_template : template python file to copy
	* run_file : location of the copied python file to be executed
"""

from os.path import join, exists, split
from bardeen.system import mkdirp, link_else_copy
from fenpei.job import Job
from settings import CALC_DIR


class PyJob(Job):

	def is_prepared(self):
		return exists(self.run_file())

	def run_template(self):
		"""
			return the path to the Python file to run (which is then copied and ran or added to a queue or something)
		"""
		return self.template_file

	def run_file(self):
		"""
			return the path to the link/copy of .run_template(), whether or not it exists
		"""
		return join(self.directory, split(self.run_template())[1])

	def prepare(self, *args, **kwargs):
		"""
			prepares the job for execution

			creates directory and copies run_template to run_file

			more steps are likely necessary for child classes
		"""
		self.status = self.PREPARED
		if not self.is_prepared():
			if self.batch_name:
				mkdirp(join(CALC_DIR, self.batch_name))
			mkdirp(self.directory)
		# todo: change this to a pure copy, and allow for processing of the file with .format() and a provided dict
		link_else_copy(self.run_template(), self.run_file())

	def start(self, node, *args, **kwargs):
		"""
			start the job and store node/pid
		"""
		self._start_pre(*args, **kwargs)
		pid = self.queue.run_job(node = node, filepath = self.run_file())
		self._start_post(node, pid, *args, **kwargs)
		return True

