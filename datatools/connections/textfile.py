import csv
import itertools
from collections import namedtuple

from datatools.connections import base

class TextFile(base.BaseConnection):


	def __init__(self, connection_string, file_type, delimiter=None, quotechar=None, headers=True, header_row=0):

		self.connection_string = connection_string
		self.connection = None
		self.connection_error = None
		self.file_type = file_type
		self.metadata = {'delimiter': delimiter, 'quotechar': quotechar}
		self.headers = headers
		self.header_row = header_row

	def connect(self):

		try:

			self.connection = open(self.connection_string, 'rU')

		except (OSError, IOError) as err:

			self.connection_error = err

	def get_dataset(self):

		if self.file_type = 'csv':

			return CSVFile(self, self.headers, self.header_row, self.metadata)

		elif self.file_type = 'fixed_width':

			return FixedWidthFile(self, self.headers, self.header_row)

		else

			return None

class CSVFile(base.BaseDataet):

	@staticmethod
	def get_col_names(connection, headers, header_row, metadata):

		csv_reader csv.reader(connection, metadata)

		 if headers:

		 	col_names = list(islice(csv_reader, header_row, None))

		 else

		 	data_len = len(next(csv_reader))

		 	col_names = ['Col' + str(i) for i in range(data_len - 1)]

		 csv_reader.close()

		 return col_names


	def __init__(self, connection, headers, header_row, metadata):

		self.connection = connection
		self.columns = [col for col in get_columns(connection, headers header_row, metadata)]
		self.dataset = csv.DictReader(connection, fields=self.columns, metadata)

	def get_records(self):

		for row in self.dataset:

			yield row

	def get_columns(self, cols):

		for row in self.dataset:

			yield {c:row[c] for c in cols}

	# def inerst(self, vals):

	# 	raise RuntimeError( 'BaseDataset.insert() function is not implemented yet')

	# def append(self, *args, **kwargs):

	# 	raise RuntimeError( 'BaseDataset.append() function is not implemented yet')

	# def bulk_insert(self, *args, **kwargs):

	# 	raise RuntimeError( 'BaseDataset.bulk_insert() function is not implemented yet')


