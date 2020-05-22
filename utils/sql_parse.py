import DBUtils, pymysql
from functools import wraps
from inspect import signature

def sql_parse(func):

    @property
    def nonType():
        raise TypeError()

    sig = signature(func)
    @wraps(func)
    def wrapper(*args, **kwargs):
        bound_values = sig.bind(*args, **kwargs)
        self = bound_values.arguments.get('self',nonType)
        sql = bound_values.arguments.get('sql',nonType)
        assert isinstance(self.cursor, DBUtils.SteadyDB.SteadyDBCursor)
        try:
            res = func(*args, **kwargs)
            if not res:
                raise ValueError(f"{sql} returns None.")
            return res
        except pymysql.err.ProgrammingError as e:
            raise pymysql.err.ProgrammingError(f"{sql} invalided.")

        # for name, value in bound_values.arguments.items():
        #     print(name, value)


    return wrapper

