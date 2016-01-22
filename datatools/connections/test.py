from datatools.connections import database, textfile


def load_db():
	cs = 'mssql+pyodbc://localdb'

	return database.Database(cs)

def load_csv():

	cs = r'C:\Users\dterrell\Projects\Python\test_data\Client Data\test_data.csv'

	return textfile.CSVFile(cs)

def load_fw():

	cs = r'C:\Users\dterrell\Projects\Python\test_data\Returned Data\email.email'
	fw = [20, 2, 282, 60, 1, 39]
	hd = ['UID', 'STA', 'SKIP1', 'EMAIL', 'MATCHIND', 'SKIP2']

	return textfile.FixedWidthFile(cs, fw, newline=False, headers=False, default_headers=hd)