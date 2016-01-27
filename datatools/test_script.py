import datetime

from datatools.connections import database, textfile
from datatools.test import *

batch_vals = {'ClientID':1, 'State': 'MT', 'Type': 'EmailAppend'}
batch_vals['Name'] = batch_vals['State'] + datetime.date.today().strftime('%m%d%y')
db = database.initialize_db(CS_DB)
csvfile = textfile.csv_reader(CS_CSV)

def insert_batch(target, vals):

	return database.table_insert(target, 'Batch', vals, returning=['BatchID'])

def append_column(reader, *cols, *vals):

	for c, v in reader:

		yield c + tuple(cols), v + tuple(vals)






