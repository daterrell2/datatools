import sqlalchemy
from sqlalchemy.sql.expression import select
#import pyodbc
from datatools.connections import base

class Database(base.BaseConnection):
	
	def __init__(self, connection_string):

		self.connection_string = connection_string
		self.engine = None
		self.connection = None
		self.metadata = None

		super(Database, self).__init__()

	def connect(self):
		try:
			self.connection = sqlalchemy.create_engine(self.connection_string).connect()
			#self.connection = self.engine.connect()
			self.metadata = sqlalchemy.MetaData(bind=self.connection.engine, reflect=True)
			# self.connection = pyodbc.connect(self.connection_string, *kwargs)
			# self.data = self.connection.cursor()
			# self.tables = {t: Table(self.connection, t) for t in self.data.tables()}

		except (sqlalchemy.exc.ArgumentError, sqlalchemy.exc.DBAPIError) as error:
			
			self.connection_error = error

	def disconnect(self):

		self.connection.close()

	def get_dataset(self, table_name, schema = None, *args, **kwargs):

		return DatabaseTable(self, table_name, schema)

	def execute_sql(self, stmt, *args, **kwargs):

		return self.connection.execute(sqlalchemy.text(stmt, *args, **kwargs))



class DatabaseTable(base.BaseDataset):

	def __init__(self, db, table_name, schema, *args, **kwargs):

		self.connection_source = db
		self.name = table_name
		self.schema = schema
		self.metadata = db.metadata
		self.dataset = sqlalchemy.Table(table_name, db.metadata, schema=schema, *args, **kwargs)
		# self.connection = self.dataset.bind.connect()
		self.columns = [c.name for c in self.dataset.columns]
		self.records = self.get_records()
		# self.column_types = [{c.name: c.type_} for c in self.dataset.columns]

		super(DatabaseTable, self).__init__()

	def select(self, cols = [], *args, **kwargs):

		if not cols:
			stmt = select([self.dataset], *args, **kwargs)

		else:
			col_list = []
			for c in cols:
				if c not in self.columns:
					return None
				else:
					col_list.append(c)
			
			stmt = select([col_list], *args, **kwargs)

		return self.connection_source.connection.execute(stmt)

	def get_records(self, *args, **kwargs):

		results = self.select(*args, **kwargs)

		for row in results:

			yield row


	# def get_record(self, *args, **kwargs):

	# 	return self.select(*args, **kwargs).fetchone()
		

	def get_columns(self, cols, *args, **kwargs):

		results = self.select(cols, *args, **kwargs)

		for row in results:

			yield row

	def get_column(self, col, *args, **kwargs):

		results = self.select([col], *args, **kwargs)

		for row in results:

			yield row

	def inerst(self, vals, *args, **kwargs):

		ins = self.dataset.insert().values(vals)

		return self.connection.execute(ins)












