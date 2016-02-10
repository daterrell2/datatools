import csv

from datatools.base_source import DBSource, FileSource
from datatools.utils import database, textfile


class TableSource(DBSource):

    def __init__(self, db, tblname, schema=None):

        super(TableSource, self).__init__(db, tblname, schema)

    def initialize(self, db, tblname, schema=None):

        self.table = database.initialize_table(db, tblname, schema)
        self.select = self.table.select()
        self.db = db


class SQLSource(DBSource):

    def __init__(self, db, sql):

        super(SQLSource, self).__init__(db, sql)

    def initialize(self, db, sql):

        self.sql = sql
        self.select = database.initialize_sql(db, sql)
        self.db = db


class CSVSource(FileSource):

    def __init__(self, path, csvparams=None, headers=True, fileparams=None):

        super(CSVSource, self).__init__(path, csvparams, headers, fileparams)

    def initialize(self, path, csvparams, headers, fileparams):

        self.path, self.fileparams = textfile.initialize_file(path, fileparams)
        self.csvparams = {} if not csvparams else csvparams
        self.headers = headers

    def _get_reader(self, f):

        return csv.reader(f, **self.csvparams)


class FixedWidthSource(FileSource):

    def __init__(self, path, fwparams=None, headers=False, fileparams=None):

        super(FixedWidthSource, self).__init__(path, fwparams,
                                               headers, fileparams)

    def initialize(self, path, fwparams, headers, fileparams):

        self.path, self.fileparams = textfile.initialize_file(path, fileparams)
        if not fwparams.get('fieldwidths'):
            raise AttributeError
        self.fwparams = fwparams

    def _get_reader(self, f):

        return textfile.parse_fw_file(f, **self.fwparams)
