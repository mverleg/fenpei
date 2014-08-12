
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
from os.path import join, basename, isdir, isfile, dirname, exists
from shutil import copyfile
from bardeen.system import mkdirp, link_else_copy
from fenpei.job import Job
from fenpei.shell import run_shell
from datetime import datetime
from time import time


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
			hard-link them (use True to prevent that with no substitutions); if you use a directory, /path/ copies
			files from it and /path copies the directory with files; directory substitutions apply to contained files
		"""
		assert ' ' not in self.run_file(), 'there should be no whitespace in run file'
		super(ShJob, self).__init__(name = name, weight = weight, batch_name = batch_name)
		timestr = datetime.now().strftime('%Y-%m-%d %H:%M') + ' (%d)' % time()
		for filepath, subst in substitutions.items():
			if isinstance(subst, Mapping):
				subst['name'] = name
				subst['now'] = timestr
		self.files = {filepath: None for filepath in self.get_files()}
		self.files.update(substitutions)
		self._fix_files()

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

			files can be either string paths or tuples of (directory, pathname); in the later case pathname will be
			copied (including directories), instead of assuming only the file is to be copied
		"""
		raise NotImplementedError()

	class FileNotFound(OSError):
		"""
			file was not found exception
		"""

	def _fix_files(self):
		"""
			check that self.files is filled with valid values (files exist etc)

			turns all string filepaths into tuples of directory and filename

			expands all directories into lists of files

			:raise ShJob.FileNotFound: subclass of OSError, indicating the one of the files doesn't exist
		"""
		def expand_dir(pre_path, post_path):
			"""
				expand a tuple (predir, postdir) into all the files in that directory
			"""
			fullpath = join(pre_path, post_path)
			subs = []
			if isdir(fullpath):
				for subpath in listdir(fullpath):
					subs.extend(expand_dir(pre_path = pre_path,
						post_path = join(fullpath, subpath).lstrip(pre_path)))
				return subs
			else:
				return [(pre_path, post_path)]

		newfiles = {}
		for filepath, subst in self.files.items():
			if not isinstance(filepath, tuple) and not isinstance(filepath, list):
				""" change string path into tuple """
				filepath = dirname(filepath), basename(filepath)
			for path_pair in expand_dir(*filepath):
				""" recursively expand a directory into all it's files """
				newfiles[path_pair] = subst
			if not exists(join(*filepath)):
				raise self.FileNotFound('"%s" is not a valid file or directory' % join(*filepath))
		self.files = newfiles

	def is_prepared(self):
		"""
			see if prepared by checking the existence of every file
		"""
		for (filedir, filename) in self.files.keys():
			if not isfile(join(self.directory, filename)):
				self._log('%s is not prepared because %s (and possibly more) are missing' % (self.name, filename), 3)
				return False
		if not isfile(self.run_file()):
			return False
		return True

	def prepare(self, *args, **kwargs):
		"""
			prepares the job for execution by copying or linking all the files, and substituting values where applicable
		"""
		self._fix_files()
		super(ShJob, self).prepare(*args, **kwargs)
		if self.is_prepared():
			return False
		for (filedir, filename), subst in self.files.items():
			filepath = join(filedir, filename)
			if exists(join(self.directory, filename)):
				self._log('%s is not prepared but already has file %s' % (self.name, filepath), 2)
				break
			mkdirp(join(self.directory, dirname(filename)))
			if bool(subst):
				""" copy files and possibly substitute """
				if isinstance(subst, Mapping):
					with open(filepath, 'r') as fhr:
						with open(join(self.directory, filename), 'w+') as fhw:
							try:
								fhw.write(fhr.read() % subst)
							except KeyError, err:
								self._log('missing key "%s" in substitution of "%s"; job not prepared' % (str(err).strip('\''), filename))
								self.cleanup()
								return False
				else:
					copyfile(filepath, join(self.directory, filename))
			else:
				""" hard-link files (directories recursively) if possible """
				link_else_copy(filepath, join(self.directory, filename))
		if isfile(join(self.directory, self.run_file())):
			run_shell(cmd = 'chmod ug+x "%s"' % join(self.directory, self.run_file()), wait = True)
		else:
			raise self.FileNotFound(('.run_file() "%s" not found after preparation; make sure it\'s origin is in ' +
				'.get_files() or in __init__ substitutions argument') % self.run_file())
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


