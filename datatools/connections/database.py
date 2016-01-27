import sqlalchemy
import sqlparse

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

def read_table(tbl, **params):
	stmt = tbl.select(**params)
	return _exec_sql(tbl.bind, stmt)

def read_sql(db, sql):
	return _exec_sql(db, sql)

def _exec_sql(db, sql):
	reader = [] if parse_sql_type(sql) != 'SELECT' else db.execute(sql)
	return ((row.keys(), row) for row in reader)

def table_insert(db, tblname, vals, schema=None, returning=[]):
	print(vals)
	with db.begin() as conn:
		meta = sqlalchemy.MetaData(bind=db)
		tbl = sqlalchemy.Table(tblname, meta, schema=schema, autoload=True)
		stmt = tbl.insert()

		if returning:
			cols = [tbl.c[col] for col in returning]
			stmt = stmt.returning(*cols)
		
		return conn.execute(stmt, *vals)

def parse_sql(stmts):

	return sqlparse.parse(stmts)

def parse_sql_type(stmts):

	parsed = parse_sql(str(stmts))

	return  parsed[0].get_type() if len(parsed) == 1 else None