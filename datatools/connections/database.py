from datatools.utils.database_utils import initialize_db, initialize_table, _select_iter

def db_connection(connection_url):

	return initialize_db(connection_url)

def table_target(db, tblname, schema=None):

	tbl = initialize_table(db, tblname, schema)

	def _write(cols, rows, return_cols=[], **kwargs):

		with db.begin() as conn:
			vals = [dict(zip(cols, rw)) for rw in rows]
			returing = [tbl.c[col] for col in return_cols]
			ins = tbl.insert().returning(*returing)
			results = conn.execute(ins, vals)
			return (dict(rs) for rs in results)

	return _write

def table_source(db, tblname, schema=None):

	tbl = initialize_table(db, tblname, schema)

	def _read(**params):
		stmt = tbl.select(**params)
		return _select_iter(db, stmt)

	return _read

def sql_source(db, sql):

	return __select_itr(db, sql)