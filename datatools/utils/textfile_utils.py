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


def parse_fw_file(f, fieldwidths):

    if hasattr(f, 'newline') and f.newline == '':
        width = sum(fieldwidths)
        rows = iter(read_file_chunck(f, width), '')
        return (parse_fw_line(rw, fieldwidths) for rw in rows)

    return (parse_fw_line(rw, fieldwidths) for rw in f)


def pad_field(val, width, align='left'):
    padding = ' ' * (width - len(val))
    padded = padding + val[0:width] + padding
    return string.lstrip(padded) if align == 'left' else string.rstrip(padded)


def pad_row(row, fieldwidths, align):
    zipped_row = zip(row, fieldwidths)
    return [pad_field(val, width, align) for val, width in zipped_row]
