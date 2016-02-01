import csv
from collections import OrderedDict
from more_itertools import peekable

from datatools.utils.textfile_utils import parse_fw_file


class FileSource():

    def __init__(self, path, fileparams=None, headers=True):

        self.path = path
        self.fileparams = {} if not fileparams else fileparams
        self.headers = headers

    def extract(self):

        with open(self.path, **self.fileparams) as f:

            reader = self._get_reader(f)
            if self.headers:
                cols = next(reader)
            else:
                cols = range(len(peekable(reader).peek()))

            for row in reader:

                yield OrderedDict(zip(cols, row))

    def columns(self):
        data = peekable(self.extract())
        return data.peek().keys()


class CSVSource(FileSource):

    def __init__(self, path, csvparams=None, *args, **kwargs):

        self.csvparams = {} if not csvparams else csvparams

        super(CSVSource, self).__init__(path, *args, **kwargs)

    def _get_reader(self, f):

        return csv.reader(f, **self.csvparams)


class FixedWidthSource(FileSource):

    def __init__(self, path, fieldwidths, headers=False, *args, **kwargs):

        self.fieldwidths = fieldwidths

        super(FixedWidthSource, self).__init__(path, headers=headers,
                                               *args, **kwargs)

    def _get_reader(self, f):

        return parse_fw_file(f, self.fieldwidths)
