import csv

from datatools.connections import base

class CSVFile(base.BaseDataset):

	def __init__(self, connection_string, headers=True, default_headers = [], params={}):

			self.headers = headers
			self.default_headers = default_headers
			self.params = params
			self.columns = None
			self.datasrc = None

			super(CSVFile, self).__init__(connection_string=connection_string)

	def load(self):
		
		if not self.params:
			self.params = {'delimiter':',', 'quotechar':'"'}

		self.datasrc = open(self.connection_string, 'rU')
		self.records = base.Reader(self._get_csv_reader())

		if not self.headers:
			self.columns = self.default_headers
		else:
			self.columns = self.records.dataset.fieldnames
		
		# self.writer = csv.DictWriter(csvfile=self.datasrc,
		# 							fieldnames=self.columns,
		# 							delimiter=self.delimiter,
		# 							quotechar=self.quotechar)

	def close(self):

		self.datasrc.close()
		self.datasrc = None

	def _get_csv_reader(self):

		if not self.headers:

			return csv.reader(self.datasrc, self.params)

		return csv.DictReader(self.datasrc, None, self.params)