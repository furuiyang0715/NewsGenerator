import DBUtils, pymysql
from functools import wraps
from inspect import signature

def sql_parse(func):

    @wraps(func)
    def wrapper(*args, **kwargs):

        try:
            res = func(*args, **kwargs)
            if not res:
                raise ValueError(f"SQL Returns None.")
            return res
        except pymysql.err.ProgrammingError as e:
            raise pymysql.err.ProgrammingError(f"SQL invalided.")

        # for name, value in bound_values.arguments.items():
        #     print(name, value)


    return wrapper

