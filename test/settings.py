
"""
	settings file for testing fenpei
	(fenpei is not supposed to be used by itself,
	so only the testing part has a settings file)
"""

# todo: standardize

from tempfile import gettempdir
from getpass import getuser
from os import makedirs
from os.path import join, dirname, abspath


BASE_DIR = abspath(dirname(__file__))

TMP_DIR = join(gettempdir(), getuser())
try:
	makedirs(TMP_DIR)
except OSError:
	pass

CALC_DIR = join(BASE_DIR, 'data')


