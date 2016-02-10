import pytest
import sqlalchemy

from .local_config import TBL
from .utils import random_string
from datatools import source, target
from datatools.utils import database

NUMROWS = range(100)
NUMCOLS = range(6)
TEST_TBL = '''
    CREATE TABLE "%s"
    ("Col0" varchar
        ,"Col1" varchar
        ,"Col2" varchar
        ,"Col3" varchar
        ,"Col4" varchar
        ,"Col5" varchar
    )
    '''


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
    with db.begin() as conn:

        for i in NUMROWS:
            data = [random_string(10, 50) for i in NUMCOLS]
            conn.execute(sql % TBL, data)


def make_fake_data():

    return ({i: random_string(10, 50) for i in NUMCOLS} for rw in NUMROWS)


def test_connect_to_db():
    cs = 'sqlite://'
    assert isinstance(database.connect_to_db(cs),
                      sqlalchemy.engine.base.Engine)


def test_initialize_table(db):

    assert isinstance(database.initialize_table(db, TBL),
                      sqlalchemy.sql.schema.Table)
    with pytest.raises(KeyError):
        database.initialize_table(db, 'does_not_exist')


def test_initialize_sql(db):
    good_sql = 'SELECT * FROM %s' % TBL
    bad_sql = 'ALTER TABLE %s ADD "newcol" varchar' % TBL

    assert isinstance(database.initialize_sql(db, good_sql), str)
    with pytest.raises(AttributeError):
        database.initialize_sql(db, bad_sql)


def test_execute_sql(db):
    sql = 'SELECT * FROM %s' % TBL

    assert isinstance(database.execute_sql(db, sql),
                      sqlalchemy.engine.result.ResultProxy)
    assert len([i for i in database.execute_sql(db, sql)]) == len(NUMROWS)


def test_compile_insert(db):

    meta = sqlalchemy.MetaData(bind=db, reflect=True)
    tbl = meta.tables[TBL]
    ins = database._compile_insert(tbl, **{'returning': ['Col0']})

    assert ins._returning == [tbl.c['Col0']]


def test_table_source(db):

    tbl = source.TableSource(db, TBL)
    assert list(tbl.columns()) == ['Col0', 'Col1', 'Col2', 'Col3', 'Col4', 'Col5']
    assert len([rw for rw in tbl.extract()]) == len(NUMROWS)

    records = set(tuple(rw) for rw in db.execute('SELECT * FROM %s' % TBL))
    test_records = set(tuple(rw.values()) for rw in (tbl.extract()))

    assert test_records == records

    with pytest.raises(KeyError):

        tbl = source.TableSource(db, 'does_not_exist')


def test_sql_source(db):

    sql = source.SQLSource(db, 'SELECT * FROM %s' % TBL)
    assert list(sql.columns()) == ['Col0', 'Col1', 'Col2', 'Col3', 'Col4', 'Col5']
    assert len([rw for rw in sql.extract()]) == len(NUMROWS)

    records = set(tuple(rw) for rw in db.execute('SELECT * FROM %s' % TBL))
    test_records = set(tuple(rw.values()) for rw in (sql.extract()))

    assert test_records == records

    with pytest.raises(AttributeError):

        sql = source.SQLSource(db, 'alter table %s add "newcol" varchar' % TBL)


def test_table_target(db):

    tbltarget = target.TableTarget(db, TBL)
    data = make_fake_data()
    zipped = (dict(zip(tbltarget.columns(), rw.values())) for rw in data)
    tbltarget.load(zipped)

    # Make sure new data was inserted
    test_len = len([rw for rw in db.execute('SELECT * FROM %s' % TBL)])
    assert test_len == len(NUMROWS) * 2

    # Returning not support by sqlite driver
    # assert set(col['Col0'] for col in ret_col) == set(d[0] for d in data)
