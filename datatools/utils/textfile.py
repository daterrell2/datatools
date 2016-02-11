class FixedWidthWriter:

    def __init__(self, fwfile, fieldwidths, align='left'):

        self.fwfile = fwfile
        self.fieldwidths = fieldwidths
        self.align = align
        self.rowcount = 0

    def writerow(self, row):
        if not self.rowcount == 0:
            self.fwfile.write('\n')
        row = ''.join(pad_row(row, self.fieldwidths, self.align))
        self.fwfile.write(row)
        self.rowcount += 1


def initialize_file(path, fileparams=None):
    params = {} if not fileparams else fileparams
    try:
        f = open(path, **params)
        f.close()
        return path, params

    except IOError as err:
        raise err


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


def parse_fw_file(f, fieldwidths, *args):

    if hasattr(f, 'newline') and f.newline == '':
        width = sum(fieldwidths)
        rows = iter(read_file_chunck(f, width), '')
        return (parse_fw_line(rw, fieldwidths) for rw in rows)

    return (parse_fw_line(rw, fieldwidths) for rw in f)


def pad_field(val, width, align='left'):
    val = str(val)
    padding = ' ' * (width - len(val))
    padded = padding + val[0:width] + padding
    return padded.lstrip() if align == 'left' else padded.rstrip()


def pad_row(row, fieldwidths, align='left', *args):
    zipped_row = zip(row, fieldwidths)
    return [pad_field(val, width, align) for val, width in zipped_row]
