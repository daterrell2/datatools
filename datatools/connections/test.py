from datatools.connections import database, textfile


def load_db():
	cs = 'mssql+pyodbc://localdb'
	tbl = 'Data'
	schema = None

	return database.DatabaseTable(cs, tbl, schema)

def load_csv():

	cs = r'C:\Users\dterrell\Projects\Python\test_data\Client Data\test_data.csv'

	return textfile.CSVFile(cs)