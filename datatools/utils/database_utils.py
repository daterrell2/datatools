import sqlparse

def _exec_with_transaction(fn):

	def func_with_transaction(db, stmt, *args, **kwargs):
		with db.begin() as trans:

			return fn(trans, stmt, *args, **kwargs)

	return func_with_transaction


def _exec_select(db, sql):
	reader = [] if parse_sql_type(sql) != 'SELECT' else db.execute(sql)
	return ((row.keys(), row) for row in reader)

@_exec_with_transaction
def _exec_insert_with_trans(db, ins, *vals, **kwargs):
	return db.execute(ins, *vals, **kwargs)

def parse_sql(stmts):

	return sqlparse.parse(stmts)

def parse_sql_type(stmts):

	parsed = parse_sql(str(stmts))

	return  parsed[0].get_type() if len(parsed) == 1 else None