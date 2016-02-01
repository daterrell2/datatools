from datatools.database import database


class TableTarget():

    def __init__(self, db, tblname, schema=None):

        self.table = database.initialize_table(db, tblname, schema)

    def load(self, source, **params):
        eng = self.table.bind
        with eng.begin() as conn:
            ins = _compile_insert(self.table, **params)
            vals = [rw for rw in source]
            results = conn.execute(ins, vals)
            return (dict(rs) for rs in results)


def _compile_insert(tbl, **params):

    if 'returning' in params.keys():

        params['returning'] = [tbl.c[col] for col in params['returning']]

    return tbl.insert(**params)
