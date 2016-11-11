
import sys
from os import getcwd
from subprocess import Popen, PIPE

"""
	Keep a list of processes to stop them from being terminated if their reference goes out of scope.
"""
process_memory = []


# only tested with wait = True
def run_shell(cmd, wait):
	if wait:
		process = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
		outp, err = process.communicate()
		if err:
			sys.stderr.write(err.strip())
			return None
		return outp
	else:
		devnull = open('/dev/null', 'w')
		process = Popen(cmd, shell=True, stdout=devnull, stderr=sys.stderr)
		process_memory.append(process)
	return None


def run_cmds_on(cmds, node, wait=True, queue=None):
	"""
	Run several commands on a local or remote machine.

	:param cmds: list of command lines, non Popen style words
	:param node:
	:param wait:
	:param queue:
	:return: list of stdout for each command if succesful, None otherwise
	:raise: no exceptions; writes to sys.stderr for problems

	commands need to be merged because otherwise the state of cd is forgotten
	"""
	split_str = '#%&split_here&%#'
	cmds = [cmd.strip() for cmd in cmds]
	cmds = [cmd[:-1] if cmd.endswith(';') else cmd for cmd in cmds]
	cmd_str = ('; echo \'%s\'; ' % split_str).join(cmds)
	cmd_str = cmd_str.replace('\\"', '"').replace('"', '\\"').replace('&; ', '& ')
	if node is None:
		cmd_str = ('bash -c "%s"' % cmd_str).replace('\n', '')
	else:
		cmd_str = ('ssh %s "%s"' % (node, cmd_str)).replace('\n', '')
	if queue:
		queue._log(cmd_str.replace('echo \'%s\'; ' % split_str, ''), level=3)
	raw_outp = run_shell(cmd_str, wait=wait)
	if raw_outp is None:
		return None
	outp = [block.strip() for block in raw_outp.split(split_str)]
	return outp


def run_cmds(cmds, wait=True, queue=None):
	return run_cmds_on(cmds, node=None, wait=wait, queue=queue)


def git_current_hash():
	def getit():
		process = Popen('git rev-parse --verify HEAD', shell=True, stdout=PIPE, stderr=PIPE)
		outp, err = process.communicate()
		if err:
			return '[no git commit found]'
		return outp.strip()
	setattr(git_current_hash, '_CACHE', getattr(git_current_hash, '_CACHE', {}))
	if getcwd() not in git_current_hash._CACHE:
		git_current_hash._CACHE[getcwd()] = getit()
	return git_current_hash._CACHE[getcwd()]


# if __name__ == '__main__':
# 	print(git_current_hash())


