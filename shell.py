
import sys
from subprocess import Popen, PIPE


'''  keep a list of processes to stop them from being terminated if their reference goes out of scope '''
process_memory = []

# only tested with wait = True
def run_shell(cmd, wait):
    if wait:
        process = Popen(cmd, shell = True, stdout = PIPE, stderr = PIPE)
        outp, err = process.communicate()
        if err:
            sys.stderr.write(err.strip())
            return None
        return outp
    else:
        devnull = open('/dev/null', 'w')
        process = Popen(cmd, shell = True, stdout = devnull, stderr = sys.stderr)
        process_memory.append(process)
    return None

def run_cmds_on(cmds, node, wait = True, queue = None):
    #if node == 'localhost':
    #    node = None
    split_str = '#%&split_here&%#'
    cmds = [cmd.strip() for cmd in cmds]
    cmds = [cmd[:-1] if cmd.endswith(';') else cmd for cmd in cmds]
    cmd_str = ('; echo \'%s\'; ' % split_str).join(cmds)
    cmd_str = cmd_str.replace('\"', '"').replace('"', '\"').replace('&; ', '& ')
    if node is None:
        cmd_str = ('bash -c "%s"' % cmd_str).replace('\n', '')
    else:
        cmd_str = ('ssh %s "%s"' % (node, cmd_str)).replace('\n', '')
    if queue:
        queue.log(cmd_str.replace('echo \'%s\'; ' % split_str, ''), level = 3)
    raw_outp = run_shell(cmd_str, wait = wait)
    if raw_outp is None:
        return None
    outp = [block.strip() for block in raw_outp.split(split_str)]
    return outp

''' commands need to be merged because otherwise the state of cd is forgotten '''
def run_cmds(cmds, wait = True, queue = None):
    return run_cmds_on(cmds, node = None, wait = wait, queue = queue)



