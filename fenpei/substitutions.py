
from collections import Mapping


# todo: this is just a placeholder for future refactoring
raise NotImplementedError
# todo: * parameter checks
# todo: * derived parameters
# todo: * required parameters


class Substitutions(Mapping):
	def __init__(self, defaults=None, **subs):
		# todo: simplistic implementation
		if defaults is not None:
			self.extend(defaults)
		self._parameters = subs
		# self._parameters = deepcopy(defaults)
		# self._parameters.update(subs)
	
	def extend(self, substitutions):
		assert isinstance(substitutions, Substitutions)
		for name, value in substitutions.items():
			if name not in self._parameters:
				self._parameters[name] = value
	
	def substitute(self, txt):
		# strings, files?
		return txt.format(**self)
	
	def __iter__(self):
		for name in self._parameters:
			yield name
	
	def __len__(self):
		return len(self._parameters)

	def __getitem__(self, item):
		#todo: error handling
		return self._parameters[item]
	
	#todo: etc


