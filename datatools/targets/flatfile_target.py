import csv
from more_itertools import peekable

from datatools.utils.textfile_utils import pad_row


class FileTarget():

    def __init__(self, path, fileparams=None, headers=True):

        self.path = path
        self.fileparams = {'mode': 'w'} or fileparams

    def load(self, source):
        vals = peekable(source)
        cols = vals.peek().keys()

        return self._write(cols, vals)


class CSVTarget(FileTarget):

    def __init__(self, path, csvparams=None, *args, **kwargs):

        self.csvparams = {} if not csvparams else csvparams

        super(CSVTarget, self).__init__(path, *args, **kwargs)

    def _write(self, cols, vals):

        with open(self.path, **self.fileparams) as f:
            writer = csv.writer(f, **self.csvparams)

            if self.headers:
                writer.writerow(cols)

            num_rows = 0

            for i, row in enumerate(vals):
                writer.writerow(row)
                num_rows += i

            return num_rows


class FixedWidthTarget(FileTarget):

    def __init__(self, path, fieldwidths, fwparams=None, *args, **kwargs):

        self.fieldwidths = fieldwidths
        self.fwparams = {} if not fwparams else fwparams

        super(FixedWidthTarget, self).__init__(path, *args, **kwargs)

    def _write(self, cols, vals):

        with open(self.path, **self.fileparams) as f:

            if self.headers:
                f.write(''.join(pad_row,
                                cols,
                                self.fieldwidths,
                                **self.fwparams))
            num_rows = 0

            for i, row in enumerate(vals):
                f.write(''.join(pad_row(row,
                                        self.fieldwidths,
                                        **self.fwparams)))
                num_rows += 1

            return num_rows
