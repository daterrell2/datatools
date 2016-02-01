import sqlalchemy
from sqlalchemy.sql.expression import text

from datatools.utils.database_utils import parse_sql_type


def connect_to_db(url):
    try:
        return sqlalchemy.create_engine(url)

    except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DBAPIError) as err:
        raise err


def initialize_table(db, tblname, schema=None, *cols):
    meta = sqlalchemy.MetaData(bind=db)
    return sqlalchemy.Table(tblname, meta,
                            schema=schema,
                            autoload=True,
                            include_columns=cols)


def initialize_sql(db, sql):
    sql_select = None if parse_sql_type(sql) != 'SELECT' else db.execute(sql)
    return sql_select


def execute_sql(db, sql, **params):

    stmt = text(sql, **params)
    with db.begin() as conn:
        results = conn.execute(stmt)
        return results
