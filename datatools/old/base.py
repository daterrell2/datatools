import collections
from contextlib import contextmanager

class BaseDataset(collections.Iterator):

	def __init__(self, *args, **kwargs):

		self.records = None
		self.columns = None
		self.datasrc = None

		self.load()

	def __next__(self):

		return next(self._read())

	def _read(self):

		for rw in self.records:

			yield dict(rw)

	def load(self):

		self.set_datasrc()
		self.set_records()

	def close(self):

		self.datasrc.close()
		self.datasrc = None

	def reload(self):

		self.close()
		self.load()

	@contextlib
	def get_datasrc(self):

		return self.datasrc

	def set_datasrc(self):

		raise RuntimeError('BaseDataset.set_datasrc() function is not implemented yet')

	@contextlib
	def get_records(self):

		return self.records

	def set_records(self):

		raise RuntimeError('BaseDataset.set_records() function is not implemented yet')


# class Reader(object):

# 	def __init__(self, dataset, columns):

# 		self.dataset = dataset
# 		self.schema = collections.namedtuple('DataRecord', columns)		

# 	def __iter__(self):

# 		return self._read()

# 	def __next__(self):

# 		return next(self._read())

# 	def _read(self):

# 		for row in map(self.schema._make, self.dataset):

# 			yield row

# 	def _dump(self):

# 		return [row for row in self._read()]

# 	def _dump_dict(self):

# 		return map(_asdict, self._dump())


# class Writer(object):

# 	def __init__(self, dataset):

# 		pass

# 	def _writerow(self, vals):

# 		pass

# 	def _writerows(self, vals):

# 		pass