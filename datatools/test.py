from datatools.connections import database, textfile

CS_DB = 'mssql+pyodbc://localdb'
CS_CSV = r'C:\Users\dterrell\Projects\Python\test_data\Client Data\test_data.csv'
CS_FW = r'C:\Users\dterrell\Projects\Python\test_data\Returned Data\email.email'
widths = [20, 2, 282, 60, 1, 39]

f1 = textfile.initialize_file(CS_CSV)
f2 = textfile.initialize_file(CS_FW)

def test_db():
	global CS_DB
	db = database.initialize_db(CS_DB)
	tbl = database.initialize_table(db, 'Batch')
	src = database.read_table(tbl)

	return db, tbl, src

def test_csv():
	global f1
	return textfile.read_csv(f1)

def test_fw():
	global f2
	global widths
	return textfile.read_fixedwidth(f2, widths, newline=False)

def finalize():
	global f1
	global f2
	map(textfile.finalize_file, [f1, f2])
