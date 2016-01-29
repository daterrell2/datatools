from time import strftime, strptime


def to_numeric(val):

    return int(val)


def to_string(val, size=None, truncate=True):

    new_val = str(val)

    if size and len(new_val) > size:
        if truncate:
            return str(val)[:size]
        else:
            raise ValueError('Conversion would truncate data')

    return str(val)


def string_to_date(val, fmt, out_format=None):

    new_date = strptime(val, fmt)

    return new_date if not out_format else strftime(new_date, out_format)
