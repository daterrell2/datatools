import collections
import functools

def transform_chain(source, transforms):

	destination = functools.reduce(run_trasform, y(x), transforms, source)
	return destination

def run_trasform(data, func):

	return func(data)

def build_chain(*funcs):

	return [func for func in funcs]
