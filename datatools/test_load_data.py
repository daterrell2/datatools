import datetime

from datatools.sources import flatfile_source
from datatools.targets import db_target
from datatools.database import database

from datatools.transforms import transforms

CS_DB = 'mssql+pyodbc://localdb'
CS_CSV = r'C:\Users\dterrell\Projects\Python\test_data\Client Data\test_data.csv'

#Batch Params:

batch_cols = ['ClientID', 'State', 'Type', 'Name']
batch_vals = [1, 'VA', 'EmailAppend',
              'VA' + datetime.date.today().strftime('%m%d%y')]

batch_data = [dict(zip(batch_cols, batch_vals))]
returning = {'returning': 'BatchID'}


# load data sources
db = database.connect_to_db(CS_DB)
batch_tbl_target = db_target.TableTarget(db, 'Batch')
data_tbl_target = db_target.TableTarget(db, 'Data')
header_tbl_target = db_target.TableTarget(db, 'Headers')

csv_src = flatfile_source.CSVSource(CS_CSV)

# insert the new batch
newbatch = batch_tbl_target.load(batch_data, **returning)
batchid = next(newbatch)['BatchID']

# map columns from csv to db
csv_headers = list(csv_src.columns())

required_cols = ['UID', 'State']
column_map = {'BatchID': batchid}
for col in csv_headers:
    if col in required_cols:
        column_map[col] = col
    else:
        column_map[col] = 'Col' + col.index()


# type conversion for UID and State
conversion_map = {'UID': lambda x: int(x), 'State': lambda x: str(x)[:2]}


# load headers to Headers table
header_vals = {'H' + str(i): csv_headers[i] for i in range(len(csv_headers)-1)}
header_vals['BatchID'] = batchid
header_tbl_target.load([header_vals])

# set up transforms to csv data
transform_append_id = transforms.append_column('BatchID', batchid)
transform_convert = transforms.map_func(conversion_map)
transform_map_cols = transforms.map_cols(column_map)

all_transforms = [transform_append_id, transform_convert, transform_map_cols]

raw_data = transforms.transform_chain(csv_src.extract(), all_transforms)


# finally, load transformed data to Data table in db

# data_tbl_target.load(raw_data)
