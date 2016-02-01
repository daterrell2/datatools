import datetime

from datatools.sources import flatfile_source
from datatools.targets import db_target, flatfile_target
from datatools.database import database

from datatools.transforms import transforms


def insert_batch(target, cols, vals):

    batch_data = [dict(zip(cols, vals))]

    return target.load(batch_data, **{'returning': ['BatchID']})

CS_DB = 'mssql+pyodbc://localdb'
CS_CSV = r'C:\Users\dterrell\Projects\Python\test_data\Client Data\test_data.csv'
# CS_FW = r'C:\Users\dterrell\Projects\Python\test_data\Returned Data\NHIA011916_EMAIL.email'
# CS_CSV2 = r'C:\Users\dterrell\Projects\Python\test_data\To Append\test_ouput.txt'

# FW_WIDTHS = [20, 2, 282, 60, 1, 39]

batch_cols = ['ClientID', 'State', 'Type', 'Name']
batch_vals = [1, 'VA', 'EmailAppend',
              'VA' + datetime.date.today().strftime('%m%d%y')]

db = database.connect_to_db(CS_DB)
batch_tbl_target = db_target.TableTarget(db, 'Batch')
data_tbl_target = db_target.TableTarget(db, 'Data')
header_tbl_target = db_target.TableTarget(db, 'Headers')

csv_src = flatfile_source.CSVSource(CS_CSV)
# fw_src = flatfile_source.FixedWidthSource(CS_FW, FW_WIDTHS)
# pipe_delim_target = flatfile_target.CSVTarget(CS_CSV2, {'delimiter': '|'})


csv_headers = list(csv_src.columns())
headers = {'H' + str(i): csv_headers[i] for i in range(len(csv_headers)-1)}
column_map = {csv_headers[i]: 'Col' + str(i) for i in range(len(csv_headers)-1)}

column_map['UID'] = 'DMI_UID'
column_map['State'] = 'DMI_State'
column_map['BatchID'] = 'BatchID'

conversion_map = {'UID': lambda x: int(x), 'State': lambda x: str(x)[:2]}

newbatch = insert_batch(batch_tbl_target, batch_cols, batch_vals)
batchid = next(newbatch)['BatchID']

headers['BatchID'] = batchid
header_tbl_target.load([headers])

transform_append_id = transforms.append_column('BatchID', batchid)
transform_convert = transforms.map_func(conversion_map)
transform_map_cols = transforms.map_cols(column_map)

csv_transforms = [transform_append_id, transform_convert, transform_map_cols]

raw_data = transforms.transform_chain(csv_src.extract(), csv_transforms)

# data_tbl_target.load(raw_data)





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
