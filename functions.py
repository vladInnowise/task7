import pandas as pd
import requests
import time
import os, os.path
from os import listdir
from os.path import isfile, join

"""the following functions represent steps of ETL process"""


# 1st function checks the folder for any updates. Global variables are used instead of any database, hope that doesn't hurt
filenum = 0
file_dict = {}
def file_checker():
    global filenum
    global file_dict
    # path to the directory with pasted files
    directory = './files_added'
    # list of csv files in the directory
    list_with_filenames = [f for f in os.listdir(directory) if isfile(join(directory, f)) and f.endswith('.csv')]
    # test if the folder is empty
    if len(list_with_filenames) == 0:
        return None
    # test if file is a duplicate
    for file_name in list_with_filenames:
        if file_name in file_dict.values():
            continue
        else:
            file_dict[filenum] = file_name
            filenum += 1
            return file_name


# extract. reads the file and prettifies it
def read_file(filename):
    # reading the file
    df = pd.read_csv(f'./files_added/{filename}')
    # renaming columns
    df.columns = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model',
                  'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date',
                  'publisher_id', 'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr',
                  'ccpa', 'country_code', 'date']
    # changing datatype
    df['date'] = pd.to_datetime(df['date'], unit='s')
    return df


# transform. parses dataframe to get result every hour
# returns message with amount of errors in file
def result_hours(df, to_csv=False):
    # Series object with required result
    result_hour = df.query('severity == "Error"')\
        .groupby([pd.Grouper(key='date', freq='60Min', dropna=True), 'bundle_id'])['severity'].count()
    # number of errors
    rh = result_hour[result_hour > 10].count()
    # просто удалить можно эти 2 строчки
    if to_csv:
        result_hour[result_hour > 10].to_csv('./results_of_parsing/result_hours.csv')
    return rh


# parses dataframe to get result in each 10 minutes
# returns message with amount of errors in file
def result_minutes(df, to_csv=False):
    # Series object with required result
    result_minute = df.query('severity == "Error"').groupby(pd.Grouper(key='date', freq='1Min', dropna=True))['severity'].count()
    # number of errors
    rm = result_minute[result_minute > 10].count()
    # честно, просто по приколу делал. Для проверки, что csv правильно работает. Чтобы при необходимости можно было куда-нибудь
    # отправлять этот csv
    if to_csv:
        result_minute[result_minute > 10].to_csv('./results_of_parsing/result_minutes.csv')
    return rm


# load. simple function to write some text to my account in telegram
def write_tg(text):
    TOKEN = '5538362126:AAFycHEQD9TjBHDrkaeKWAIlREusughrvD8'
    METHOD = 'sendMessage'
    CHAT_ID = '730906913'
    return requests.get(f'http://api.telegram.org/bot{TOKEN}/{METHOD}?chat_id={CHAT_ID}&text={text}')
