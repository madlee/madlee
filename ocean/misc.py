"""utility functions"""

########################################################################
### Folder and file operations
from os.path import split as split_path, join as join_path, normpath as normalize_path, abspath as abs_path
from os.path import isdir as is_dir, isfile as is_file
from os import makedirs as make_dirs, listdir as list_dir
from os import remove as remove_file
from os import access as access_file_permission
from os import W_OK, R_OK, X_OK, F_OK

def ensure_dirs(name):
    if not is_dir(name):
        make_dirs(name)


def split_filename(filename):
    folder, filename = split_path(filename)
    tokens = filename.split('.')
    filename = tokens[:-1]
    extname = tokens[-1]
    return folder, '.'.join(filename), extname


def writable_file(filename):
    if access_file_permission(filename, W_OK):
        return True
    else:
        return False


### Folder and file operation
########################################################################



########################################################################
###  String related

from re import compile as compile_re
DIGITS = compile_re(r'\d+')
def all_digits(v):
    result = DIGITS.findall(v)
    return ''.join(result)


TRUE_VALUES = {'t', 'true', 'y', 'yes', '1'}
FALSE_VALUES = {'f', 'false', 'n', 'no', '0'}
def to_bool(v):
    v = v.lower()
    if v in FALSE_VALUES:
        return False
    elif v in TRUE_VALUES:
        return True
    else:
        raise ValueError(v)

###  String related
########################################################################




########################################################################
###  Date Time related
# from datetime import datetime as DateTime, date as Date, timedelta as TimeDelta, time as Time
from datetime import datetime as DateTime, timedelta as TimeDelta, time as Time
from datetime import date as Date
from bisect import bisect, bisect_left, bisect_right
from time import sleep

ONE_SECOND  = TimeDelta(seconds=1)
ONE_MINUTE  = TimeDelta(seconds=60)
ONE_HOUR    = TimeDelta(seconds=3600)
ONE_DAY     = TimeDelta(1)

def today():
    return to_date(Date.today())

def now():
    return DateTime.now()

def to_date(date, default=None):
    if date == None or date == 'null' or date == '':
        return default
    else:
        date = all_digits(str(date))
        date = date[0:8]
        date = int(date)
        year = date // 10000
        month = (date // 100) % 100
        day = date % 100
        return Date(year, month, day)


def to_date_or_today(date):
    return to_date(date, DateTime.today())


def to_date_or_none(date):
    return to_date(date, None)


def to_time(time):
    time_d = all_digits(str(time))
    if len(time_d) <= 4:
        time = int(time_d+'00')
    elif len(time_d) > 6:
        time = float(time)

    hour = int(time) // 10000
    minute = int(time) % 10000 // 100
    seconds = time - hour*10000 - minute*100
    return Time(hour, minute, seconds)


def uniform_date(date):
    try:
        return date.year*10000+date.month*100+date.day
    except AttributeError:
        return int(date)


def yesterday():
    return Date.today()-ONE_DAY


class Timer(object):
    def __init__(self):
        self.reset()
    
    def __str__(self):
        return str(self.delta)

    @property
    def delta(self):
        return DateTime.now() - self.__start

    def reset(self):
        self.__start = DateTime.now()

    def to_seconds(self):
        return self.delta.total_seconds()




###  Date Time related
########################################################################




########################################################################
###  Database Related

def execute_sqls(sql, cursor):
    '''Execute multi SQL seperated by ';' '''
    for i in sql.split(';'):
        i = i.strip()
        if i:
            cursor.execute(i)

###  Database Related
########################################################################




########################################################################
### IO related

def print_bar(char='=', width=78):
    print (char*width)

def print_title(title, char='=', width=78):
    print_bar(char, width)
    for line in title.split('\n'):
        line = line.strip()
        n0 = width - len(line) - 6
        n1 = n0 // 2
        n2 = n0-n1
        print (char, ' '*n1, line, ' '*n2, char)
    print_bar(char, width)

def print_subtitle(title, char='-', width=78):
    for line in title.split('\n'):
        line = line.strip()
        n0 = width - len(line) - 2
        n1 = n0 // 2
        n2 = n0-n1
        print (' '*n1, line, ' '*n2)
    print_bar(char, width)


from pickle import loads as load_pickle, dumps as dump_pickle
from json import loads as load_json, dumps as dump_json


### IO related
########################################################################




########################################################################
### Simple Algorithm
from random import random

def random_split(total, count, dec=None, min=0.5):
    mean = total/count
    result = [mean*(min + (1-min)*2*random()) for _ in range(count)]
    factor = total/sum(result)
    result = [i*factor for i in result]
    if dec is not None:
        result = [round(i, dec) for i in result]

    return result




def data_bin(data, factor, key=0, i=0, j=None, fillgap=False):
    if j == None:
        j = len(data)
    if i < j:
        ki, kj = data[i][key] // factor,  data[j-1][key] // factor
        if ki == kj:
            return [ data[i:j] ]
        else:
            result = []
            mid = (i+j) // 2

            result_l = data_bin(data, factor, key, i, mid)
            result_r = data_bin(data, factor, key, mid, j)
            if result_l and result_r:
                vl, vr = result_l[-1], result_r[0]
                if vl[0][key] // factor == vr[0][key] // factor:
                    result = result_l[:-1] + [vl+vr] + result_r[1:]
                else:
                    result = result_l + result_r
            else:
                result = result_l + result_r

        if fillgap and result:
            result2 = [result[0]]
            next = 1
            for next in range(1, len(result)):
                next_k = result[next][0][key] // factor
                result2 += [[]] * int(next_k - ki - 1)
                result2.append(result[next])
                ki = next_k
            result = result2

        return result
    else:
        return []


### Simple Algorithm
########################################################################

