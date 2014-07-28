
"""
	extended base class for fenpei job that runs through an executable (incl. shell script);
	this should be considered abstract

	as with :ref:Job, your custom job(s) should inherit from this job and extend the relevant methods.
	Instead of :ref: prepare and :ref: start, you can override:

	* get_files
	* run_file
"""

from collections import Mapping
from os import listdir
from os.path import join, basename, isdir, isfile
from shutil import copyfile
from bardeen.system import mkdirp, link_else_copy
from fenpei.job import Job
from fenpei.shell import run_shell


class ShJob(Job):

	def __init__(self, name, substitutions, weight = 1, batch_name = None):
		"""
			create a executable or shell job object, provided a number of files or directories which will be copied,
			and (optionally) substitutions for each of them

			:param substitutions: a dictionary; keys are files or directories to be copied, values are dicts of
			substitutions, or None; e.g. {'run.sh': {'R': 15, 'method': 'ccsd(t)'}} will copy
			:raise ShJob.FileNotFound: subclass of OSError, indicating the files argument contains data that is invalid

			for other parameters, see :ref: Job

			files will be copied if bool(files) is True (for substritutions), otherwise it will be attempted to
			hard-link them (use True to prevent that with no substitutions); directories can not have substitutions;
			if you use a directory, /path/ copies files from it and /path copies the directory with files
		"""
		assert ' ' not in self.run_file(), 'there should be no whitespace in run file'
		super(ShJob, self).__init__(name = name, weight = weight, batch_name = batch_name)
		for filepath, subst in substitutions.items():
			if isinstance(subst, Mapping):
				subst['name'] = name
		self.files = {filepath: None for filepath in self.get_files()}
		self.files.update(substitutions)
		self._check_files()

	@classmethod
	def get_files(cls):
		"""
			:return: the list of files and directories used by this code, which will be linked or copied

			substitutions for non-static files should be supplied to the constructor
		"""
		raise NotImplementedError()

	@classmethod
	def run_file(cls):
		"""
			:return: the path to the file which executes the job; should be in get_files()
		"""
		raise NotImplementedError()

	class FileNotFound(OSError):
		"""
			file was not found exception
		"""

	def _check_files(self):
		"""
			check that self.files is filled with valid values (files exist etc)

			:raise ShJob.FileNotFound: subclass of OSError, indicating the one of the files doesn't exist
		"""
		for filepath, subst in self.files.items():
			if isdir(filepath):
				assert bool(subst) is False
			elif isfile(filepath):
				if not isinstance(subst, Mapping):
					self.files[filepath] = bool(subst)
			else:
				raise self.FileNotFound('%s is not a valid file or directory' % filepath)

	def is_prepared(self):
		"""
			see if prepared by checking the existence of every file
		"""
		for filepath in self.files.keys():
			if isdir(filepath):
				for filesubpath in listdir(filepath):
					# todo: doesn't work with more than one level of directories
					if not isfile(join(self.directory, basename(filepath), filesubpath)):
						return False
			else:
				if not isfile(join(self.directory, basename(filepath))):
					return False
		if not isfile(self.run_file()):
			return False
		return True

	def prepare(self, *args, **kwargs):
		"""
			prepares the job for execution by copying or linking all the files, and substituting values where applicable
		"""
		self._check_files()
		super(ShJob, self).prepare(*args, **kwargs)
		for filepath, subst in self.files.items():
			if bool(self.files):
				""" copy files and possibly substitute """
				if isinstance(subst, Mapping):
					with open(filepath, 'r') as fhr:
						with open(join(self.directory, basename(filepath)), 'w+') as fhw:
							fhw.write(fhr.read() % subst)
				else:
					copyfile(filepath, join(self.directory, basename(filepath)))
			else:
				""" hard-link files (directories recursively) if possible """
				if isdir(filepath):
					mkdirp(join(self.directory, basename(filepath)))
					# todo: does this work for more than one level of directories? does that copy? (should link)
					for filesubpath in listdir(filepath):
						link_else_copy(filepath, join(self.directory, basename(filepath), filesubpath))
				else:
					link_else_copy(filepath, join(self.directory, basename(filepath)))
		if isfile(join(self.directory, self.run_file())):
			run_shell(cmd = 'chmod ug+x "%s"' % join(self.directory, self.run_file()), wait = True)
		else:
			raise self.FileNotFound('.run_file() "%s" not found after preparation; make sure it\'s origin is in \
				.get_files() or in __init__ substitutions argument' % self.run_file())
		return True

	def start(self, node, *args, **kwargs):
		"""
			start the job and store node/pid
		"""
		self._start_pre(*args, **kwargs)
		cmd = 'nohup ./%s &> out.log &' % self.run_file()
		pid = self.queue.run_cmd(job = self, cmd = cmd)
		self._start_post(node, pid, *args, **kwargs)
		return True


