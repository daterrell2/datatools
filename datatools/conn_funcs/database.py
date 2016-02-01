from collections import OrderedDict

from datatools.utils.database_utils import initialize_db, initialize_table
from datatools.utils.database_utils import parse_sql_type


def db_connection(connection_url):

    return initialize_db(connection_url)


def table_target(db, tblname, schema=None, **kwargs):

    tbl = initialize_table(db, tblname, schema)

    def _write(source):

        with db.begin() as conn:
            ins = _compile_insert(tbl, **kwargs)
            results = conn.execute(ins, [rw for rw in source])

            return (dict(rs) for rs in results)

    return _write


def table_source(db, tblname, schema=None):

    tbl = initialize_table(db, tblname, schema)

    def _read(**params):
        stmt = tbl.select(**params)
        return _select_iter(db, stmt)

    return _read


def sql_source(db, sql):

    return _select_iter(db, sql)


def _select_iter(db, sql):
    reader = [] if parse_sql_type(sql) != 'SELECT' else db.execute(sql)
    return (OrderedDict(row) for row in reader)


def _compile_insert(tbl, **kwargs):

    if 'columns' in kwargs.keys():

        kwargs['columns'] = [tbl.c[col] for col in kwargs['columns']]

    return tbl.ins(**kwargs)
