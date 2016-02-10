import sqlparse


def parse_sql(stmts):

    return sqlparse.parse(stmts)


def parse_sql_type(stmts):

    parsed = parse_sql(str(stmts))

    return parsed[0].get_type() if len(parsed) == 1 else None
