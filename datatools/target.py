import csv

from datatools.utils import database, textfile


class TableTarget:

    def __init__(self, db, tblname, schema=None, cols=None):

        self.initialize(db, tblname, schema=schema)

    def initialize(self, db, tblname, schema=None, cols=None):

        self.table = database.initialize_table(db, tblname, schema=schema)
        self._columns = cols or [col.name for col in self.table.c]

    def load(self, source, **params):
        eng = self.table.bind
        with eng.begin() as conn:
            ins = database._compile_insert(self.table, **params)
            vals = [rw for rw in source]
            results = conn.execute(ins, vals)
            return (dict(rs) for rs in results)

    def columns(self):

        return self._columns


class FileTarget:

    def __init__(self, path, fileparams=None, headers=True):

        self.path = path
        self.fileparams = {'mode': 'w'} or fileparams
        self.headers = headers

        if not textfile.validate_path(self.path, **self.fileparams):
            raise IOError

    def load(self, source):
        cols = source.columns()
        vals = (rw.values() for rw in source.extract())

        with open(self.path, **self.fileparams) as f:
            writer = self._get_writer(f)

            if self.headers:
                writer.writerow(cols)

            num_rows = 0

            for i, row in enumerate(vals):
                writer.writerow(row)
                num_rows += i

            return num_rows


class CSVTarget(FileTarget):

    def __init__(self, path, csvparams=None,
                 headers=True, fileparams={'mode': 'w'}):

        super(CSVTarget, self).__init__(path, csvparams, headers, fileparams)

    def initialize(self, path, csvparams, headers, fileparams):

        self.path, self.fileparams = textfile.initialize_file(path, fileparams)
        self.csvparams = {} if not csvparams else csvparams
        self.headers = headers

    def _get_writer(self, f):

        return csv.writer(f, **self.csvparams)


class FixedWidthTarget(FileTarget):

    def __init__(self, path, fieldwidths,
                 fwparams=None, headers=False,
                 *args, **kwargs):

        self.fieldwidths = fieldwidths
        self.fwparams = {} if not fwparams else fwparams

        super(FixedWidthTarget, self).__init__(path=path,
                                               headers=headers,
                                               *args, **kwargs)

    def initialize(self, path, fwparams, headers, fileparams):

        self.path, self.fileparams = textfile.initialize_file(path, fileparams)
        if not fwparams.get('fieldwidths'):
            raise AttributeError
        self.fwparams = fwparams

    def _get_writer(self, f):

        return textfile.FixedWidthWriter(f, **self.fwparams)
