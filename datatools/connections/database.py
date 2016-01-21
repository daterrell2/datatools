import sqlalchemy
from datatools.connections import base

class DatabaseTable(base.BaseDataset):

	def __init__(self, connection_string, tablename=None, schema=None, *args, **kwargs):

		self.table = tablename
		self.schema = schema
		self.datasrc = None
		self.metadata = None
		self.columns = []

		super(DatabaseTable, self).__init__(connection_string = connection_string)

	def load(self):

		self.datasrc = sqlalchemy.create_engine(self.connection_string).connect()
		self.metadata = sqlalchemy.MetaData(bind=self.datasrc.engine)

		if self.table:

				self.columns = [col.name for col in self.get_table().columns]
				tbl_select = self.get_table().select()
				tbl_ins = self.get_table().insert()
				#self.columns = [col.name for col in self.data.columns]
				self.records = base.Reader(self.execute(tbl_select), self.columns)
				#self.writer = base.Writer(self.execute(tbl_ins))

	def close(self):

		self.datasrc.close()
		self.datasrc = None

	def refresh(self):

		tbl_select = self.get_table().select()
		conn = self.data_src
		self.records = base.Reader(conn(tbl_select))

	def get_table(self):

		return sqlalchemy.Table(self.table, self.metadata, schema=self.schema, autoload=True)

	def execute(self, sql):

		results = self.datasrc.execute(sql)

		return results

	def execute_sql(self, sql, *args, **kwargs):

		return self.datasrc.execute(sqlalchemy.text(sql, *args, **kwargs))