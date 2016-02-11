import pytest
import sqlalchemy
import csv

from .local_config import *
from .utils import make_fake_data
from datatools.utils.textfile import FixedWidthWriter

CSV_PATH = FILES['CSV_SRC']
FW_PATH = FILES['FW_SRC']


@pytest.fixture(scope='module')
def csv_with_headers():
    path = CSV_PATH
    with open(path, 'w', newline='\n') as f:
        csvfile = csv.writer(f)
        populate_file(csvfile, headers=True)

    return path


@pytest.fixture(scope='module')
def fw_with_headers():
    path = FW_PATH
    with open(path, 'w') as f:
        fwfile = FixedWidthWriter(f, WIDTHS)
        populate_file(fwfile, headers=True)

    return path


@pytest.fixture(scope='module')
def db(request):
    '''
    Connects to db from local_config
    '''
    newdb = sqlalchemy.create_engine('sqlite://')
    add_tbl(newdb)
    populate_tbl(newdb)

    def finalize():
        newdb.dispose()

    request.addfinalizer(finalize)

    return newdb


def add_tbl(db):
    sql = TEST_TBL
    db.execute(sql % TBL)


def populate_tbl(db):

    sql = 'INSERT INTO %s VALUES (' + ','.join(['?' for i in NUMCOLS]) + ')'
    data = make_fake_data(NUMCOLS, NUMROWS)
    with db.begin() as conn:
        for rw in data:
            conn.execute(sql % TBL, list(rw.values()))


def populate_file(file_obj, headers=True):

    data = make_fake_data(NUMCOLS, NUMROWS)
    if headers:
        file_obj.writerow(['Col' + str(i) for i in NUMCOLS])

    for rw in data:
        file_obj.writerow(list(rw.values()))
