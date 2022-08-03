import os
import os.path
from os.path import isfile, join

import pandas as pd
import requests

import config

"""the following functions represent steps of ETL process"""


"""these global variables replace any no-sql DB, for example redis"""
filenum = 0
file_dict = {}


"""1st function checks the folder for any updates. Global variables are used instead of any database, hope that doesn't hurt"""


def file_checker() -> str:
    global filenum
    global file_dict
    """path to the directory with pasted files"""
    directory = './files_added'
    """list of csv files in the directory"""
    list_with_filenames = [f for f in os.listdir(directory) if isfile(join(directory, f)) and f.endswith('.csv')]
    """test if the folder is empty"""
    if len(list_with_filenames) == 0:
        return None
    """test if file is a duplicate"""
    for file_name in list_with_filenames:
        if file_name in file_dict.values():
            continue
        else:
            file_dict[filenum] = file_name
            filenum += 1
            return file_name


"""extract. reads the file and prettifies it"""


def read_file(filename: str) -> pd.DataFrame:
    """reading the file"""
    df = pd.read_csv(f'./files_added/{filename}')
    """renaming columns"""
    df.columns = config.columns
    """changing datatype"""
    df['date'] = pd.to_datetime(df['date'], unit='s')
    return df


"""transform. parses dataframe to get result every hour.
returns message with amount of errors in file"""


def result_hours(df: pd.DataFrame, to_csv=False) -> int:
    """Series object with required result"""
    result_hour = df.query('severity == "Error"')\
        .groupby([pd.Grouper(key='date', freq='60Min', dropna=True), 'bundle_id'])['severity'].count()
    """number of errors"""
    rh = result_hour[result_hour > 10].count()
    """просто удалить можно эти 2 строчки"""
    if to_csv:
        result_hour[result_hour > 10].to_csv('./results_of_parsing/result_hours.csv')
    return rh


"""parses dataframe to get result in each 10 minutes
returns message with amount of errors in file"""


def result_minutes(df: pd.DataFrame, to_csv=False) -> int:
    """Series object with required result"""
    result_minute = df.query('severity == "Error"').groupby(pd.Grouper(key='date', freq='1Min', dropna=True))['severity'].count()
    """number of errors"""
    rm = result_minute[result_minute > 10].count()
    """честно, просто по приколу делал. Для проверки, что csv правильно работает. Чтобы при необходимости можно было куда-нибудь
    отправлять этот csv"""
    if to_csv:
        result_minute[result_minute > 10].to_csv('./results_of_parsing/result_minutes.csv')
    return rm


"""load. simple function to write some text to my account in telegram"""


def write_tg(text: str):
    return requests.get(f'{config.link}{text}')
