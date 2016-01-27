import csv
from datatools.connections import record

def csv_reader(path, headers=True, **params):
	columns = ()
	with open(path, 'rU') as csvfile:
		csvreader = csv.reader(csvfile, **params)
		if headers:
			columns = tuple(csvreader.next())
		for row in record.make_record(csvreader, columns):
			yield row


def fixedwidth_reader(path, fieldwidths, headers=True, newline=True):
	columns = None
	with open(path, 'rU') as fwfile:
		fwreader = parse_fw_file(fwfile, fieldwidths, newline)
		if headers:
			columns = tuple(next(fwreader))
		for row in record.make_record(fwreader, columns):
			yield row

def fw_dict_reader(fwreader, columns):
	return (dict(zip(columns, rw)) for rw in fwfile)

def fixedwidth_writer(path, fieldwidths, header_vals, rows, newline='\n', align='left'):

	with open(path, 'wU') as fwfile:

		for row in header_vals + rows:
			writerow = pad_field(row[i], fieldwidths[i], align) for i in range(len(rows)-1)
			fwfile.write(''.join(writerow + [newline]))

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

def pad_field(val, width, align='left'):
	
	if len(val) >= width:
		return val[0:width]
	
	padding = ' ' * (width - len(val))
	
	if align == 'right':
		return padding + val

	return val + padding
