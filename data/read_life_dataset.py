import numpy as np
import pandas as pd

from datetime import datetime
from datetime import timezone
from zipfile import ZipFile
import os


def make_df(start_time, end_time, path = 'data'):
    timestamps = sorted(os.listdir(os.path.join(path, 'timestamps')))
    start_timestamp, end_timestamp = make_timestamps_from_datetime(start_time, end_time, timestamps)
    interval = make_interval(start_timestamp, end_timestamp, timestamps)
    try:
        with ZipFile(os.path.join(path, 'timestamps.zip')) as timestamps_zip:
            df_list = [pd.read_csv(timestamps_zip.open("timestamps/" + file), header=None,
                                   names=['fullVisitorId', 'url_id', 'visitStartTime']) for file in interval]
    except:
        df_list = [pd.read_csv(os.path.join(path, 'timestamps/') + file, header=None, names=['fullVisitorId', 'url_id', 'visitStartTime'])
               for file in interval]

    df = pd.concat(df_list)
    labels, levels = pd.factorize(df['fullVisitorId'])
    df['user_id'] = labels
    df.set_index(['user_id', 'url_id'], inplace=True, drop=True)
    return df


# first = 12/03/2017 07:00:00, last = 14/04/2017 11:11:29 1491818423 1491991225
def make_timestamps_from_datetime(start_time, end_time, timestamps):
    if start_time == 'first':
        start_timestamp = timestamps[0]
    else:
        start_datetime = datetime.strptime(start_time, '%d/%m/%Y %H:%M:%S')
        start_timestamp = (start_datetime - datetime(1970, 1, 1)).total_seconds()

    if end_time == 'last':
        end_timestamp = timestamps[-1]
    else:
        end_datetime = datetime.strptime(end_time, '%d/%m/%Y %H:%M:%S')
        end_timestamp = (end_datetime - datetime(1970, 1, 1)).total_seconds()
    return (start_timestamp, end_timestamp)


def make_interval(start_timestamp, end_timestamp, timestamps):
    start_timestamp = str(start_timestamp)
    end_timestamp = str(end_timestamp)
    interval = [t for t in timestamps if t >= start_timestamp and t <= end_timestamp]
    return interval


# Using texts.csv to make urls for each url_id
def make_urls_df(path = 'data'):
    texts = pd.read_csv(os.path.join(path, 'texts.csv'))
    tag_cleaned = texts['tag'].str.split().str.get(0)
    texts['tag_cleaned'] = tag_cleaned
    texts['url_id'] = texts['url_id'].astype(str)
    texts['pagePath'] = '/t/' + texts['tag_cleaned'] + '/' + texts['url_id']
    texts.set_index(['url_id'], inplace=True)
    urls = texts.drop(['subtitle', 'tag', 'title', 'tag_cleaned'], axis=1)
    return urls


def merge_df(df, urls):
    df.reset_index(level=['url_id'], inplace=True)
    df.reset_index(level=['user_id'], inplace=True)
    urls.reset_index(level=['url_id'], inplace=True)
    urls['url_id'] = urls['url_id'].astype(int)
    df['fullVisitorId'] = df['fullVisitorId'].astype(str)
    df_result = pd.merge(df, urls, on='url_id', how='left')
    labels, levels = pd.factorize(df_result['url_id'])
    df_result['url_id'] = labels
    df_result.set_index(['user_id', 'url_id'], inplace=True)
    df_result.sort_index(inplace=True)
    return df_result

if __name__ == "__main__":
    df = make_df('15/03/2017 10:00:00', '16/03/2017 10:00:00')
    urls = make_urls_df()
    df_result = merge_df(df, urls)