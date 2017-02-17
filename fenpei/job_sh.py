
"""
Extended base class for fenpei job that runs through an executable (incl. shell script);
this should be considered abstract.

As with :ref:Job, your custom job(s) should inherit from this job and extend the relevant methods.
Instead of :ref: prepare and :ref: start, you can override:

* get_files
* run_file
"""

from socket import gethostname
from collections import Mapping
from os import listdir, symlink
from os.path import join, basename, isdir, isfile, dirname, exists, islink
from shutil import copyfile
from bardeen.system import mkdirp
from fenpei.job import Job
from fenpei.shell import run_shell
from datetime import datetime
from time import time

from fenpei.utils import substitute, FormattingException
from .shell import git_current_hash


def extend_substitutions(subst, name, batch, directory, git_hash=None):
	timestr = datetime.now().strftime('%Y-%m-%d %H:%M') + ' (%d)' % time()
	if git_hash is None:
		git_hash = git_current_hash()
	if isinstance(subst, Mapping):
		subst['name'] = name
		subst['batch_name'] = batch
		subst['now'] = timestr
		subst['directory'] = directory
		subst['hostname'] = gethostname()
		subst['git_commit'] = git_hash
	elif subst is None:
		pass
	else:
		raise NotImplementedError('I haven\'t thought about this, maybe it\'s not needed anyway.')


class ShJob(Job):

	def __init__(self, name, substitutions, weight=1, batch_name=None, formatter='jinja2', use_symlink=True, force_node=None):
		"""
		Create a executable or shell job object, provided a number of files or directories which will be copied,
		and (optionally) substitutions for each of them.

		:param substitutions: a dictionary; keys are files or directories to be copied, values are dicts of
		substitutions, or None; e.g. {'run.sh': {'R': 15, 'method': 'ccsd(t)'}} will copy
		:raise ShJob.FileNotFound: subclass of OSError, indicating the files argument contains data that is invalid

		For other parameters, see :ref: Job.

		Files will be copied if bool(files) is True (for substitutions), otherwise it will be attempted to
		hard-link them (use True to prevent that with no substitutions); if you use a directory, /path/ copies
		files from it and /path copies the directory with files; directory substitutions apply to contained files.
		"""
		assert ' ' not in self.run_file(), 'there should be no whitespace in run file'
		super(ShJob, self).__init__(name=name, weight=weight, batch_name=batch_name, force_node=force_node)
		git_commit = git_current_hash()
		for filepath, subst in substitutions.items():
			extend_substitutions(subst, name, batch_name, self.directory, git_hash=git_commit)
		self.files = {filepath: None for filepath in self.get_files()}
		self.files.update(substitutions)
		self.formatter = formatter
		self.use_symlink = use_symlink
		self.files = self._fix_files(self.files)

	@classmethod
	def get_files(cls):
		"""
		:return: the list of files and directories used by this code, which will be linked or copied

		Substitutions for non-static files should be supplied to the constructor.
		"""
		raise NotImplementedError()

	@classmethod
	def run_file(cls):
		"""
		:return: the path to the file which executes the job; should be in get_files()

		Files can be either string paths or tuples of (directory, pathname); in the later case pathname will be
		copied (including directories), instead of assuming only the file is to be copied.
		"""
		raise NotImplementedError()

	class FileNotFound(OSError):
		"""
		File was not found exception.
		"""

	def _fix_files(self, files):
		"""
		Check that self.files is filled with valid values (files exist etc).

		Turns all string filepaths into tuples of directory and filename.

		Expands all directories into lists of files.

		:raise ShJob.FileNotFound: subclass of OSError, indicating the one of the files doesn't exist
		"""
		def expand_dir(pre_path, source_pth, target_pth):
			"""
			Expand a tuple (predir, postdir) into all the files in that directory.
			"""
			fullpath = join(pre_path, source_pth)
			subs = []
			if isdir(fullpath):
				for subpath in listdir(fullpath):
					subs.extend(expand_dir(pre_path=pre_path,
						source_pth= join(fullpath, subpath).lstrip(pre_path),
						target_pth=target_pth))
				return subs
			else:
				return [(pre_path, source_pth, target_pth)]

		newfiles = {}
		for filepath, subst in files.items():
			if not isinstance(filepath, tuple) and not isinstance(filepath, list):
				""" change string path into tuple """
				filepath = dirname(filepath), basename(filepath), basename(filepath)
			if len(filepath) == 2:
				filepath = filepath[0], filepath[1], filepath[1]
			for pth_triple in expand_dir(*filepath):
				""" recursively expand a directory into all it's files """
				newfiles[pth_triple] = subst
			if not exists(join(*filepath[:2])):
				raise self.FileNotFound('"%s" is not a valid file or directory' % join(*filepath[:2]))
		return newfiles

	def is_prepared(self):
		"""
		See if prepared by checking the existence of every file.
		"""
		for fromroot, frompth, topth in self.files.keys():
			if not isfile(join(self.directory, topth)) and not islink(join(self.directory, topth)):
				self._log('{0:s} is not prepared because {1:s} (and possibly more) are missing'
					.format(self.name, frompth), 3)
				return False
		if not isfile(join(self.directory, self.run_file())):
			return False
		return True

	def prepare(self, *args, **kwargs):
		"""
		Prepares the job for execution by copying or linking all the files, and substituting values where applicable.
		"""
		super(ShJob, self).prepare(*args, **kwargs)
		if self.is_prepared():
			return False
		for (fromroot, frompth, topth), subst in self.files.items():
			sourcefilepath = join(fromroot, frompth)
			destfilepath = join(self.directory, topth)
			if exists(destfilepath):
				self._log('{0:s} is not prepared but already has file {1:s}'.format(self.name, destfilepath), 2)
				break
			mkdirp(join(self.directory, dirname(topth)))
			if subst:
				""" copy files and possibly substitute """
				if isinstance(subst, Mapping):
					with open(sourcefilepath, 'r') as fhr:
						with open(destfilepath, 'w+') as fhw:
							inp = fhr.read()
							try:
								if hasattr(self.formatter, '__call__'):
									# noinspection PyCallingNonCallable
									outp = self.formatter(inp, subst, job=self, filename=sourcefilepath)
								else:
									outp = substitute(inp, subst, formatter=self.formatter, job=self, filename=sourcefilepath)
								fhw.write(outp)
							except FormattingException as err:
								self._log('{0:}'.format(err))
								self.cleanup()
								return False
				else:
					if self.use_symlink:
						symlink(sourcefilepath, destfilepath)
					else:
						copyfile(sourcefilepath, destfilepath)
			else:
				""" Soft-link files if possible and allowed by settings """
				if self.use_symlink:
					symlink(sourcefilepath, destfilepath)
				else:
					copyfile(sourcefilepath, destfilepath)
		if isfile(join(self.directory, self.run_file())):
			run_shell(cmd = 'chmod ug+x "%s"' % join(self.directory, self.run_file()), wait = True)
		else:
			raise self.FileNotFound(('.run_file() "%s" not found after preparation; make sure it\'s origin is in ' +
				'.get_files() or in __init__ substitutions argument') % self.run_file())
		return True

	def start(self, node, *args, **kwargs):
		"""
		Start the job and store node/pid.
		"""
		self._start_pre(*args, **kwargs)
		""" nohup, bg and std redirect should be handeled by queue """
		cmd = './{0:s}'.format(self.run_file())  # no abspath, not necessary, and is publicly visible in queue
		pid = self.queue.run_cmd(job=self, cmd=cmd)
		self._start_post(node, pid, *args, **kwargs)
		return True


