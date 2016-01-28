import sqlparse
import sqlalchemy

def initialize_db(connection_url):
	try:
		return sqlalchemy.create_engine(connection_url)

	except (sqlalchemy.exc.SQLAlchemyError, sqlalchemy.exc.DBAPIError) as err:
		raise err

def initialize_table(db, tblname, schema=None, *cols):
	meta = sqlalchemy.MetaData(bind=db)
	return sqlalchemy.Table(tblname, meta, 
							schema=schema, 
							autoload=True, 
							include_columns=cols)


def _select_iter(db, sql):
	reader = [] if parse_sql_type(sql) != 'SELECT' else db.execute(sql)
	return (dict(row) for row in reader)

def _exec_with_transaction(fn):

	def func_with_transaction(db, stmt, *args, **kwargs):
		with db.begin() as trans:

			return fn(trans, stmt, *args, **kwargs)

	return func_with_transaction

@_exec_with_transaction
def _exec_insert_with_trans(trans, ins, *vals, **kwargs):
	return trans.execute(ins, *vals, **kwargs)

def parse_sql(stmts):

	return sqlparse.parse(stmts)

def parse_sql_type(stmts):

	parsed = parse_sql(str(stmts))

	return  parsed[0].get_type() if len(parsed) == 1 else None