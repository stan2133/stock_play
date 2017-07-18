#!/usr/bin/python3
# encoding:utf-8
# author: Yuguang Liu
import os
import requests
import zipfile
import shutil
import glob
import pandas as pd
import psutil
import time
from itertools import repeat


_url = 'http://www.cninfo.com.cn/cninfo-new/data/download'
# post_data = {
#     "market": (None, "sz"),
#     "type": (None, "lrb"),
#     "code": (None, "000858"),
#     "orgid": (None, "gssz000858"),
#     "minYear": (None, "2014"),
#     "maxYear": (None, "2017"),
#     "cw_code": (None, "000858")
# }


def get_post_data(code, file_type, min_year, max_year):
    code = str(code)
    if code[0] == '0':
        market = 'sz'
    else:
        market = 'sh'
    post_data = {
        "market": (None, market),
        "type": (None, file_type),
        "code": (None, code),
        "orgid": (None, "gs" + market + code),
        "minYear": (None, str(min_year)),
        "maxYear": (None, str(max_year)),
        "cw_code": (None, code)
    }
    return post_data


def remove(path):
    """ param <path> could either be relative or absolute. """
    if os.path.isfile(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(path))


def csv_into_pd():
    df_collect = []
    for file in glob.glob('./test/*.csv'):
        print(file)
        df_collect.append(pd.read_csv(file, encoding='gbk'))
    df = pd.concat(df_collect)
    return df


def download_data(code, file_type, min_year, max_year):
    post_data = get_post_data(code, file_type, min_year, max_year)
    time.sleep(0.5)
    res = requests.post(_url, files=post_data)
    # print(res.status_code)
    try:
        if res and res.status_code:
            with open('./test.zip', 'wb') as f:
                f.write(res.content)
        with zipfile.ZipFile("./test.zip", "r") as zip_ref:
            zip_ref.extractall("./test/")
        remove('./test.zip')
    except Exception:
        pass

if __name__ == "__main__":
    pid = os.getpid()
    py = psutil.Process(pid)
    code_file = pd.read_excel('code.xlsx', header=None)
    code_file.columns = ['code']
    code_file.code = code_file.code.apply(lambda x: x.split('.')[0])
    code_list = code_file.code.values
    # print(code_list[0:10])
    del code_file
    num = len(code_list)
    download_data('600001', 'lrb', '2014', '2017')
    # result = list(map(download_data, code_list[0:num], repeat('lrb', num), repeat('2014', num), repeat('2017', num)))
    memoryUse = py.memory_info()[0]/2.**30  # memory use in GB...I think
    print('memory use:', memoryUse)
