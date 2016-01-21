import csv
import itertools

from datatools.connections import base

def read_file_chunck(f, chunck):

	def read_chunck():
		return f.read(chunck)

	return read_chunck



def slices(text, fieldwidths):

	pos = 0
	for width in fieldwidths:
		yield text[pos:pos + width]
		pos += width

def parse_fw_line(line, fieldwidths):

	return list(slices(line, fieldwidths))

def parse_fw_file(f, fieldwidths):

	width = sum(fieldwidths)
	rows = iter(read_file_chunck(f, width), '')
	return (parse_fw_line(rw, fieldwidths) for rw in rows)

class TextFile(base.BaseDataset):

	def __init__(self, connection_string, headers=True, default_headers = []):

			self.headers = headers
			self.default_headers = default_headers
			self.columns = None
			self.datasrc = None

			super(TextFile, self).__init__(connection_string=connection_string)

	def load(self):

		self.datasrc = open(self.connection_string, 'rU')
		self.records = base.Reader(self._get_reader(), self.columns)

	def close(self):

		self.datasrc.close()
		self.datasrc = None

	def _get_reader(self):

		raise RuntimeError('TextFile._get_reader() function is not implemented yet')

class CSVFile(TextFile):

	def __init__(self, connection_string, headers=True, default_headers = [], params={'delimiter':',', 'quotechar':'"'}):

			self.params = params

			super(CSVFile, self).__init__(connection_string=connection_string, headers=headers, default_headers=default_headers)

	def _get_reader(self):

		csv_reader = csv.reader(self.datasrc, self.params)

		if not self.headers:
			self.columns = self.default_headers

		else:
			self.columns = next(csv_reader)
			
		return csv_reader

class FixedWidthFile(TextFile):

	def __init__(self, connection_string, fieldwidths, headers=True, default_headers = []):

		self.fieldwidths = fieldwidths

		super(FixedWidthFile, self).__init__(connection_string=connection_string, headers=headers, default_headers=default_headers)


	def _get_reader(self):

		fw_reader = parse_fw_file(self.datasrc, self.fieldwidths)

		if not self.headers:
			self.columns = self.default_headers

		else:
			self.columns = next(fw_reader)

		return fw_reader

