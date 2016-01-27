import string

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
		return (parse_fw_line(rw, fieldwidths) for rw in f)

	else:
		width = sum(fieldwidths)
		rows = iter(read_file_chunck(f, width), '')
		return (parse_fw_line(rw, fieldwidths) for rw in rows)

def pad_field(val, width, align='left'):
	
	padding = ' ' * (width - len(val))
	padded = padding + val[0:width] + padding
	return string.lstrip(padded) if align == 'left' else string.rstrip(padded)

def pad_row(row, align, newline):
	return [pad_field(*val, align) for val in row] + [newline]

def convert_to_fw(data, fieldwidths, align='left', newline='\n'):
	zipped_data = (zip(rw, fieldwidths) for rw in data)
	return (pad_row(rw, align, newline) for rw in zipped_data)