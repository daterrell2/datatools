import pytest
import csv

from .fixtures import csv_with_headers, fw_no_headers
from .local_config import NUMROWS, NUMCOLS, WIDTHS, FILES
from .utils import make_fake_data
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


def test_fw_src(fw_no_headers):

    path = fw_no_headers
    fw_src = source.FixedWidthSource(path, {'fieldwidths': WIDTHS})
    fw_compare = open(path).read().splitlines()

    assert len(list(fw_src.extract())) == len(NUMROWS)
    for i, row in enumerate(fw_src.extract()):
        assert ''.join(list(row.values())) == fw_compare[i]


def test_fixed_width_writer():
    path = FILES['FW_SRC']
    widths = [100, 100]
    data = make_fake_data(range(len(widths)), range(1))
    data = list(next(data).values())
    padded_data = [v[:w] + (' ' * (w - len(v))) for v, w in zip(data, widths)]

    with open(path, 'w') as f:
        fw_writer = textfile.FixedWidthWriter(f, widths)
        fw_writer.writerow(data)
    assert ''.join(padded_data) == open(path).read()
