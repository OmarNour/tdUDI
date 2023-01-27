import pandas as pd
import numpy as np
import traceback
import functools
from datetime import datetime
from pandasql import sqldf
import configparser
import concurrent.futures
import time
from collections import namedtuple

SPECIAL_CHARACTERS = [
    '!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ','
    , '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '['
    , '\\', ']', '^', '_', '`', '{', '|', '}', '~'
]
PI_TEMPLATE = """PRIMARY INDEX ( {pi_cols} )"""
COL_DTYPE_TEMPLATE = """\t{comma}{col_name}  {data_type}{precision} CHARACTER SET {latin_unicode} {not_case_sensitive} CASESPECIFIC {not_null}\n """
DDL_TABLE_TEMPLATE = """ 
CREATE {set_multiset} TABLE {schema_name}.{table_name}
    ,FALLBACK
    ,NO BEFORE JOURNAL
    ,NO AFTER JOURNAL
    ,CHECKSUM = DEFAULT
    ,DEFAULT MERGEBLOCKRATIO
(\n{col_dtype})
{pi_index}
{si_index}
 """
DDL_VIEW_TEMPLATE = """CREATE VIEW /*VER.1*/  {schema_name}.{view_name} AS LOCK ROW FOR ACCESS {query_txt}"""


def list_to_string(list, separator=None, quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator

    to_string = prefix.join((single_quotes(str(x)) if quotes == 1 else str(x)) if x is not None else "" for x in list)
    return to_string


def generate_run_id():
    return int(str(time.time()).replace('.', ''))


def single_quotes(string):
    return "'%s'" % string.replace("'", '"')


def time_elapsed_decorator(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        x = function(*args, **kwargs)
        print(f'Time elapsed for {function.__name__}: {datetime.now() - start_time} \n')
        return x

    return wrapper


def Logging_decorator(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except:
            print("Error!!", traceback.format_exc())

    return wrapper


def processes(target_func, iterator, max_workers=None):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(target_func, iterator)


def threads(target_func, iterator, max_workers=None):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(target_func, iterator)


def read_config_file(file):
    parser = configparser.ConfigParser()
    parser.read(file)
    return parser
