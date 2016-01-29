import csv
from collections import OrderedDict
from more_itertools import peekable

from datatools.utils.textfile_utils import pad_row, parse_fw_file


def csv_target(path, mode='wU', headers=True, csvparams={}):

    def _write(source):
        with open(path, mode) as csvfile:
            vals = peekable(source)
            cols = vals.peek().keys()
            csv_writer = csv.DictWriter(csvfile, fieldnames=cols, **csvparams)
            if headers:
                csv_writer.writeheader()
            for row in vals:
                csv_writer.writerow(row.values())

    return _write


def fixedwidth_target(path, fieldwidths, mode='wU', headers=False, **params):

    def _write(source):
        with open(path, mode) as fwfile:
            vals = peekable(source)
            if headers:
                padded_cols = pad_row(vals.peek().keys(), fieldwidths)
                fwfile.write(''.join(padded_cols))
            for row in vals:
                padded_row = pad_row(row.values, fieldwidths, **params)
                fwfile.write(''.join(padded_row))

    return _write


def csv_source(path, headers=True, fileparams={}, csvparams={}):

    def _read():
        with open(path, **fileparams) as csvfile:
            csvreader = csv.reader(csvfile, **csvparams)
            if headers:
                cols = next(csvreader)
            else:
                cols = range(len(peekable(csvreader.peek())))
            for row in csvreader:
                yield OrderedDict(zip(cols, row))
    return _read


def fixedwidth_source(path, fieldwidths, headers=True, fileparams={'mode': 'r'}):

    def _read():
        with open(path, **fileparams) as fwfile:
            fwreader = parse_fw_file(fwfile, fieldwidths)
            if headers:
                cols = next(fwreader)
            else:
                cols = range(len(peekable(fwreader).peek()))
            for row in fwreader:
                yield OrderedDict(zip(cols, row))

    return _read
