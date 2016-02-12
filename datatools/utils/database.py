import sqlalchemy
from sqlalchemy.sql.expression import text, select

from datatools.utils.sql import parse_sql_type


def connect_to_db(url):
    try:
        return sqlalchemy.create_engine(url)

    except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DBAPIError) as err:
        raise err


def initialize_table(db, tblname, schema=None):
    meta = sqlalchemy.MetaData(bind=db, reflect=True, schema=schema)
    try:
        return meta.tables[tblname]

    except KeyError as err:
        raise err


def initialize_table_source(db, tblname, schema=None, cols=None):

    tbl = initialize_table(db, tblname, schema)

    if not cols:
        return select([tbl])

    else:
        return select([tbl.c[col] for col in cols])


def initialize_sql(db, sql):
    sql_select = None if parse_sql_type(sql) != 'SELECT' else sql
    if not sql_select:
        raise AttributeError

    return sql_select


def execute_sql(db, sql, **params):

    stmt = text(sql, **params)
    with db.begin() as conn:
        results = conn.execute(stmt)
        return results


def _compile_insert(tbl, **params):

    if 'returning' in params.keys():

        params['returning'] = [tbl.c[col] for col in params['returning']]

    return tbl.insert(**params)
