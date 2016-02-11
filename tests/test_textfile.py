import pytest
import csv

from .fixtures import csv_with_headers
from .local_config import NUMROWS, NUMCOLS
from datatools.utils import textfile
from datatools import source, target


def test_initialize_file(csv_with_headers):
    path = csv_with_headers
    assert textfile.initialize_file(path) == (path, {})
    with pytest.raises(IOError):
        textfile.initialize_file('does_not_exist')


def test_csv_src(csv_with_headers):
    path = csv_with_headers
    csv_src = source.CSVSource(path)
    csv_compare = csv.reader(open(path))
    cols_compare = next(csv_compare)

    assert csv_src.columns() == cols_compare
    assert len(list(csv_src.extract())) == len(NUMROWS)
    for row in csv_src.extract():
        assert list(row.values()) == next(csv_compare)
