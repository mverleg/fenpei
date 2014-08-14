
"""
	settings file for testing fenpei
	(fenpei is not supposed to be used by itself,
	so only the testing part has a settings file)
"""


from tempfile import gettempdir
from getpass import getuser
from os import makedirs
from os.path import join, dirname, abspath, expanduser
from bardeen.system import mkdirp


BASE_DIR = abspath(dirname(__file__))

TMP_DIR = join(gettempdir(), getuser())
try:
	makedirs(TMP_DIR)
except OSError:
	pass

CALC_DIR = join(expanduser('~'), 'data/fenpei')
mkdirp(CALC_DIR)

