import datetime

from datatools.connections import database, textfile
from datatools.test import *

batch_cols = ('ClientID', 'State', 'Type', 'Name')
vals1 = (1, 'MT', 'EmailAppend')
vals2 = (vals1[1] + datetime.date.today().strftime('%m%d%y'),)
batch_vals = [vals1 + vals2]
db, tbl, src = test_db()
csvfile = test_csv()

def insert_batch(target, cols, vals):

	return database.write_to_table(target, cols, vals, return_cols=['BatchID'])

# def append_column(reader, *cols, *vals):

# 	for c, v in reader:

# 		yield c + tuple(cols), v + tuple(vals)






