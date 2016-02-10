from more_itertools import peekable
from collections import OrderedDict


class DBSource:

    def __init__(self, *args, **kwargs):

        self.initialize(*args, **kwargs)

    def initialize(self):

        pass

    def extract(self):
        records = self.db.execute(self.select)
        for row in records:

            yield OrderedDict(row)

    def columns(self):
        data = peekable(self.extract())
        return data.peek().keys()


class FileSource:

    def __init__(self, *args, **kwargs):

        self.initializae(*args, **kwargs)

    def extract(self):

        with open(self.path, **self.fileparams) as f:

            reader = self._get_reader(f)
            if self.headers:
                cols = next(reader)

            else:
                reader = peekable(reader)
                cols = range(len(reader.peek()))

            for row in reader:
                yield OrderedDict(zip(cols, row))

    def columns(self):
        data = peekable(self.extract())
        return data.peek().keys()
