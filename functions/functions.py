import os
import datetime as dt
import shutil
import sys, subprocess
import pandas as pd
import numpy as np
import traceback
import functools
from datetime import datetime
from pandasql import sqldf
import configparser
import concurrent.futures
import time
import multiprocessing
from collections import namedtuple


smx_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
scripts_path = "/Users/omarnour/Downloads/smx_scripts"

SPECIAL_CHARACTERS = [
    '!', '"', '#', '$', '%', '&', "'", '(', ')', '*', '+', ','
    , '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '['
    , '\\', ']', '^', '_', '`', '{', '|', '}', '~'
]
cast_dtype_template = """({dtype_name} {precise})"""
col_mapping_template = """{comma}{col_name} {cast_dtype} {alias}"""
from_template = """{schema_name}.{table_name}"""
PI_TEMPLATE = """PRIMARY INDEX ( {pi_cols} )"""
COL_DTYPE_TEMPLATE = """\t{comma}{col_name}  {data_type}{precision} {latin_unicode} {case_sensitive} {not_null}\n """
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


class WriteFile:
    def __init__(self, file_path, file_name, ext, f_mode="w+", new_line=False):
        self.new_line = new_line
        self.f = open(os.path.join(file_path, file_name + "." + ext), f_mode, encoding="utf-8")

    def write(self, txt, new_line=None):
        self.f.write(txt)
        new_line = self.new_line if new_line is None else None
        self.f.write("\n") if new_line else None

    def close(self):
        self.f.close()


def generate_run_id():
    return int(str(time.time()).replace('.', ''))


def open_folder(path):
    if sys.platform == "win32":
        os.startfile(path)
    else:
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, path])


def create_folder(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        remove_folder(path)
        create_folder(path)


def remove_folder(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def list_to_string(list, separator=None, quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator

    to_string = prefix.join((single_quotes(str(x)) if quotes == 1 else str(x)) if x is not None else "" for x in list)
    return to_string


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


class TemplateLogError(WriteFile):
    def __init__(self, log_error_path, file_name_path, error_file_name, error):
        self.log_error_path = log_error_path
        self.log_file_name = "log"
        self.ext = "txt"
        super().__init__(self.log_error_path, self.log_file_name, self.ext, "a+", True)
        self.file_name_path = file_name_path
        self.error_file_name = error_file_name
        self.error = error

    def log_error(self):
        error_separator = "##############################################################################"
        self.write(str(dt.datetime.now()))
        self.write(self.file_name_path)
        self.write(self.error_file_name)
        self.write(self.error)
        self.write(error_separator)


def log_error_decorator(error_log_path):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            # cf = args[0]
            # source_output_path = args[1]
            file_name = get_file_name(__file__)
            try:
                function(*args, **kwargs)
            except:
                print(traceback.format_exc()) if error_log_path is None else None
                TemplateLogError(error_log_path, '', file_name, traceback.format_exc()).log_error()
        return wrapper
    return decorator


def get_file_name(file):
    return os.path.splitext(os.path.basename(file))[0]


def processes(target_func, iterator, max_workers=multiprocessing.cpu_count()):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(target_func, iterator)


def threads(target_func, iterator, max_workers=multiprocessing.cpu_count()):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        return executor.map(target_func, iterator)


def read_config_file(file):
    parser = configparser.ConfigParser()
    parser.read(file)
    return parser
