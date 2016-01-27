import csv

from datatools.utils.textfile_utils import convert_to_fw, parse_fw_file

def initialize_file(path, mode='rU'):
	return open(path, mode)

def finalize_file(filestream):
	return filestream.close()

def write_to_csv(filestream, cols, rows, **params):
	with filestream as writer:
		csv_writer = csv.writer(writer, params)
		for line in cols + rows:
			csv_writer.writerow(line)

def write_to_fixedwidth(filestream, cols, rows, fieldwidths, **params):
	with filestream as writer:
		data = convert_to_fw(cols + rows, fieldwidths, **params)
		for line in data:
			writer.write(''.join(line))

def read_csv(filestream, headers=True, **params):
	csvreader = csv.reader(filestream, **params)
	return _read_file(csvreader, headers)

def read_fixedwidth(filestream, fieldwidths, headers=True, newline=True):
	fwreader = parse_fw_file(filestream, fieldwidths, newline)
	return _read_file(fwreader, headers)

def _read_file(reader, headers):
	cols = next(reader) if headers else []
	for row in reader:
		yield cols, row