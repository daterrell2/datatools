class BaseConnection(object):

	def __init__(self, *args, **kwargs):

		pass

	def connect(self, *args, **kwargs):

		raise RuntimeError( 'BaseConnection.open_connection() function is not implemented yet.')

	def disconnect(self, *args, **kwargs):
		
		raise RuntimeError( 'BaseConnection.close_connection() function is not implemented yet.')

	def get_dataset(self, *args, **kwargs):

		raise RuntimeError( 'BaseConnection.get_dataset() function is not implemented yet.')



class BaseDataset(object):

	def __init__(self, *args, **kwargs):

		pass

	def get_records(self, *args, **kwargs):

		raise RuntimeError( 'BaseDataset.get_records() function is not implemented yet')

	def get_record(self, *args, **kwargs):

		raise RuntimeError( 'BaseDataset.get_record() function is not implemented yet')

	def get_columns(self, *args, **kwargs):

		raise RuntimeError( 'BaseDataset.get_columns() function is not implemented yet')

	def get_column(self, *args, **kwargs):

		raise RuntimeError( 'BaseDataset.get_column() function is not implemented yet')

	def inerst(self, *args, **kwargs):

		raise RuntimeError( 'BaseDataset.insert() function is not implemented yet')

	def append(self, *args, **kwargs):

		raise RuntimeError( 'BaseDataset.append() function is not implemented yet')

	def bulk_insert(self, *args, **kwargs):

		raise RuntimeError( 'BaseDataset.bulk_insert() function is not implemented yet')