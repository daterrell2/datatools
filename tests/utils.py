import random
from string import ascii_letters, digits


def random_string(min_len, max_len, chars=ascii_letters + digits):

    str_len = random.choice(range(min_len, max_len))

    return ''.join([random.choice(chars) for i in range(str_len)])


def make_fake_data(numcols, numrows):

    return ({i: random_string(10, 50) for i in numcols} for rw in numrows)
