
from os import environ
from os.path import join, expanduser


if 'CALC_DIR' in environ:
	CALC_DIR = environ['CALC_DIR']
else:
	CALC_DIR = join(expanduser('~'), 'data/sheffield')


