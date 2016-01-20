class BaseDataset(object):

	def __init__(self, connection_string, *args, **kwargs):

		self.connection_string = connection_string
		self.records = None
		self.writer = None
		self.columns = None

	def load(self):

		raise RuntimeError( 'BaseDataset.load() function is not implemented yet')

	def close(self):

		raise RuntimeError('BaseDataset.close() function is not implemented yet')

	def reload(self):

		self.close()
		self.load()

	def read_next(self):

		if self.records:
			
			return next(self.records)

	def read_all(self):

		self.reload()
		data = self.records._dump()

		return data

	def write(self, vals):

		if self.writer:

			return self.writer.writerows(vals)

class Reader(object):

	def __init__(self, dataset):

		self.dataset=dataset

	def __iter__(self):

		return self._read()

	def __next__(self):

		return next(self._read())

	def _read(self):

		for row in self.dataset:

			yield row

	def _dump(self):

		return [row for row in self._read()]

class Writer(object):

	def __init__(self, dataset):

		pass

	def _writerow(self, vals):

		pass

	def _writerows(self, vals):

		pass