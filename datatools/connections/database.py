import sqlalchemy
from datatools.connections import base

class Database(object):

	def __init__(self, connection_string, schema=None):

		self.connection_string = connection_string
		self.schema = schema
		self.engine = sqlalchemy.create_engine(connection_string)
		self.metadata = sqlalchemy.MetaData(bind=self.engine)
		self.tables = sqlalchemy.inspect(self.engine).get_table_names(schema=schema)

	def __getitem__(self, key):

		if key not in self.tables:
			raise KeyError('%s is not a valid table.'%key)
		else:
			tbl = DatabaseTable(self, key, schema=self.schema)
		return tbl

	def connect(self):

		return self.engine.connect()

	def execute(self, stmt):

		return self.connect().execute(stmt)

	def execute_sql(self, sql, *args, **kwargs):

		return self.datasrc.execute(sqlalchemy.text(sql, *args, **kwargs))

class DatabaseTable(base.BaseDataset):

	def __init__(self, db, tablename=None, schema=None):

		self.db = db
		self.table = sqlalchemy.Table(tablename, db.metadata, schema=schema, autoload=True)		

		super(DatabaseTable, self).__init__()

	def set_datasrc(self):

		self.datasrc = self.db.connect()

	def set_records(self):
		self.columns = [col.name for col in self.table.columns]
		self.records = (rw for rw in self.get_datasrc().execute(self.table.select()))
		#self.records = base.Reader(reader, cols)


class SQLSet(base.BaseDataset):

	pass

