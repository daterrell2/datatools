import csv
from datatools.utils.textfile_utils import convert_to_fw, parse_fw_file

def csv_target(path, mode='wU'):

	def _write(cols, rows, **params):
		with open(path, mode) as csvfile:
			csv_writer = csv.writer(writer, params)
			for line in cols + rows:
				csv_writer.writerow(line)
	return _write

def fixedwidth_target(path, fieldwidths, mode='wU'):

	def _write(cols, rows, **params):
		with open(path, mode) as fwfile:
			data = convert_to_fw(cols + rows, fieldwidths, **params)
			for line in data:
				fwfile.write(''.join(line))
	return _write


def csv_source(path, mode='rU', headers=True, **params):

	def _read():
		with open(path, mode) as csvfile:
			csvreader = csv.reader(csvfile, **params)
			return _file_iter(csvreader, headers)
	return _read


def fixedwidth_source(path, fieldwidths, mode='rU', headers=True, newline=True):

	def _read():
		with open(path, mode) as fwfile:
			fwreader = parse_fw_file(fwfile, fieldwidths, newline)
			return _file_iter(fwreader, headers)
	return _read

def _file_iter(reader, headers):
	if headers:
		return (dict(zip(cols, row)) for row in reader)

	else:
		return(dict(zip(range(len(row)), row)) for row in reader)