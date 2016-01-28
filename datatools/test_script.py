import datetime

from datatools.connections import database, textfile

CS_DB = 'mssql+pyodbc://localdb'
CS_CSV = r'C:\Users\dterrell\Projects\Python\test_data\Client Data\test_data.csv'
CS_FW = r'C:\Users\dterrell\Projects\Python\test_data\Returned Data\email.email'
CS_CSV2 = r'C:\Users\dterrell\Projects\Python\test_data\To Append\test_ouput.txt'

FW_WIDTHS = [20, 2, 282, 60, 1, 39]

batch_cols = ('ClientID', 'State', 'Type', 'Name')
batch_vals = [(1, 'VA', 'EmailAppend', 'VA' + datetime.date.today().strftime('%m%d%y'))]

db = database.db_connection(CS_DB)
batch_tbl_target = database.table_target(db, 'Batch')
data_tbl_target = database.table_target(db, 'Data')
header_tbl_target = database.table_target(db, 'Headers')

csv_src = textfile.csv_source(CS_CSV)
fw_src = textfile.fixedwidth_source(CS_FW, FW_WIDTHS)
pipe_delim_target = textfile.csv_target(CS_CSV2, {'delimiter':'|'})

# def simple_col_map(src, dest):
	
# 	src_cols, dest_cols = src[0], dest[0]

# 	return zip(src_cols, dest_cols[:len(src_cols)])

# def append_column(reader, *cols, *vals):

# 	for c, v in reader:

# 		yield c + tuple(cols), v + tuple(vals)

# def insert_batch(target, cols, vals):

# 	return target(cols, vals, return_cols=['BatchID'])

# batch_id = batch_tbl_target(batch_cols, batch_vals, return_cols=['BatchID'])

# raw_data = append_column(csv_src, batch_id[0], batch_id[1])

# mapped_cols = simple_col_map(raw_data, data_tbl_target)

# raw_data = strip_headers(raw_data, header_tbl_target)
# def append_batchid(src):
# 	new_batch = insert_batch(batch_tbl, batch_cols, batch_vals)
# 	return append_column(csv_src, 'BatchID', new_batch[1][0])


# def strip_headers(reader, dest=None):

# 	cols, rows = reader
# 	if dest:
# 		dest(cols=None, rows=cols)
# 	return (), rows

# def csv_strip_headers():
# 	global csv_src
# 	dest = database.initialize_table('Headers')
# 	return strip_headers(csv_src, dest, database.write_to_table)

# def migrate_data(src, dest, writer):

# 	map_cols = simple_col_map(src, dest)

# 	return writer(dest, cols=map_cols[1], vals=src[1])