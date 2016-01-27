import sqlalchemy
import sqlparse

from datatools.utils.database_utils import _exec_select
from datatools.utils.database_utils import _exec_insert_with_trans

def initialize_db(connection_string):
	try:
		return sqlalchemy.create_engine(connection_string)

	except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DBAPIError) as err:
		raise err

def initialize_table(db, tblname, schema=None, *cols):
	meta = sqlalchemy.MetaData(bind=db)
	return sqlalchemy.Table(tblname, meta, 
							schema=schema, 
							autoload=True, 
							include_columns=cols)

def write_to_table(tbl, cols, rows, return_cols=[], **kwargs):
	vals = [dict(zip(cols, rw)) for rw in rows]
	ret_cols = [tbl.c[col] for col in return_cols]
	ins = tbl.insert().returning(*ret_cols)
	results = _exec_insert_with_trans(tbl.bind, ins, *vals, **kwargs)
	return tuple(return_cols), (rs for rs in results)

def read_table(tbl, **params):
	stmt = tbl.select(**params)
	return _exec_select(tbl.bind, stmt)

def read_sql(db, sql):
	return _exec_select(db, sql)