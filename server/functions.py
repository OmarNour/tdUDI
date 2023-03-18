from .config import *
import os
import datetime as dt
import shutil
import sys, subprocess
import pandas as pd
# import modin.pandas as pd
import swifter
from swifter import set_defaults

set_defaults(
    npartitions=100,
    dask_threshold=1,
    scheduler="processes",
    progress_bar=True,
    progress_bar_desc=None,
    allow_dask_on_strings=True,
    force_parallel=True,
)
import numpy as np
import traceback
import functools
from functools import lru_cache
from datetime import datetime
from pandasql import sqldf
import configparser
import concurrent.futures
import time
import multiprocessing
import re

try:
    import cPickle as pickle
except:
    import pickle


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


def list_to_string(_list, separator=None, quotes=0):
    if separator is None:
        prefix = ""
    else:
        prefix = separator

    to_string = prefix.join((single_quotes(str(x)) if quotes == 1 else str(x)) if x is not None else "" for x in _list)
    return to_string


def single_quotes(string):
    return "'%s'" % string  # .replace("'", '"')


# def single_quotes(string):
#     return "'%s'" % string


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
                error_detailed = f"""\n\nFunction Name: {function.__name__}\n\nargs: {args}\n\nkwargs: {kwargs}\n\nError: {traceback.format_exc()}"""
                TemplateLogError(error_log_path, '', file_name, error_detailed).log_error()

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


def remove_all_from_list(lst: [], word: str):
    return [item for item in lst if item != word]


def merge_multiple_spaces(txt: str):
    return re.sub('\s+', ' ', txt.strip())


def split_text(text, sep, maxsplit=0):
    return re.split(sep, text, flags=re.IGNORECASE, maxsplit=maxsplit)


def filter_dataframe(df: pd.DataFrame, col: str = None, filter_value=None) -> pd.DataFrame:
    if filter_value is None:
        return df
    else:
        # if col in df.index.names:
        #     pass

        if isinstance(filter_value, str):
            mask = df[col].str.lower() == filter_value.lower()
        elif isinstance(filter_value, list):
            filter_value = [x.lower() for x in filter_value if isinstance(x, str)]
            mask = df[col].str.lower().isin(filter_value)
        else:
            mask = df[col] == filter_value

        return df[mask]


if __name__ == '__main__':
    _ = """ dasda's """
    x = single_quotes(_)
    print(x)
