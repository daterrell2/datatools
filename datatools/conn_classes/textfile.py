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

def parse_fw_file(f, fieldwidths, newline=True):

	if newline:
		return (slices(rw, fieldwidths) for rw in f)

	else:
		width = sum(fieldwidths)
		rows = iter(read_file_chunck(f, width), '')
		return (parse_fw_line(rw, fieldwidths) for rw in rows)

class TextFile(base.BaseDataset):

	def __init__(self, connection_string, headers=True, default_headers = []):

			self.connection_string = connection_string
			self.headers = headers
			self.default_headers = default_headers
			self.columns = None
			self.datasrc = None

			super(TextFile, self).__init__()

	def set_datasrc(self):

		self.datasrc = open(self.connection_string, 'rU')

	def set_records(self):

		records, cols = self._get_reader()
		self.columns = cols
		self.records = (dict(zip(self.columns, r)) for r in records)

	def _get_reader(self):

		raise RuntimeError('TextFile._get_reader() function is not implemented yet')

class CSVFile(TextFile):

	def __init__(self, connection_string, headers=True, default_headers = [], params={'delimiter':',', 'quotechar':'"'}):

			self.params = params

			super(CSVFile, self).__init__(connection_string=connection_string, headers=headers, default_headers=default_headers)

	def write(self, data, cols=[]):

		with open(self.connection_string, 'wU') as csvfile:
			if not cols:
				cols = data._fields
			csvwriter = csv.DictWriter(csvfile, fieldnames=cols)

			return map(csvwriter.writerow, data._dump_dict())

	def _get_reader(self):

		csv_reader = csv.reader(self.datasrc, self.params)
		cols = []
		if not self.headers:
			cols = self.default_headers

		else:
			cols = next(csv_reader)
			
		return csv_reader, cols

class FixedWidthFile(TextFile):

	def __init__(self, connection_string, fieldwidths, newline=True, headers=True, default_headers = []):

		self.fieldwidths = fieldwidths
		self.newline = newline

		super(FixedWidthFile, self).__init__(connection_string=connection_string, headers=headers, default_headers=default_headers)


	def _get_reader(self):

		fw_reader = parse_fw_file(self.datasrc, self.fieldwidths, self.newline)
		cols = []
		if not self.headers:
			cols = self.default_headers

		else:
			cols = next(fw_reader)

		return fw_reader, cols