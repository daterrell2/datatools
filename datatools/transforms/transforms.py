import functools
from collections import OrderedDict
from more_itertools import peekable


def transform_chain(source, transforms):

    def row_transform(row, funcs=transforms):

        return functools.reduce(lambda x, y: y(x), transforms, row)

    return map(row_transform, source)


def map_cols(col_map):

    def _transform(row):
        mapped = OrderedDict()

        for k in col_map.keys():
            mapped[col_map[k]] = row[k]

        return mapped

    return _transform


def auto_map_cols(source, cols):
    src_cols = peekable(source).peek().keys()
    col_map = dict(zip(src_cols, cols))

    def _transform(row):
        return map_cols(col_map)

    return _transform


def map_func(func_map):

    def map_funcs_to_row(row):
        mapped = OrderedDict()

        for k, v in row.items():
            if k in func_map.keys():
                mapped[k] = func_map[k](v)
            else:
                mapped[k] = v
        return mapped


def append_column(name, val):

    pass
