from datatools.database import initialize_table, initialize_sql


class DBSource():

    def __init__(self):

        pass

    def extract(self):

        for row in self.select:

            yield row


class TableSource(DBSource):

    def __init__(self, db, tblname, schema=None):

        self.table = initialize_table(db, tblname, schema)
        self.select = self.table.select()

        super(TableSource, self).__init__()


class SQLSource(DBSource):

    def __init__(self, db, sql):

        self.sql = sql
        self.select = initialize_sql(db, sql)

        super(TableSource, self).__init__()
