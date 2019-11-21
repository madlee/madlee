
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

SECONDS_IN_AN_HOUR = 60 * 60
SECONDS_IN_A_DAY   = 24 * SECONDS_IN_AN_HOUR 


def today():
    return to_date(Date.today())


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


SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY = 6, 0, 1, 2, 3, 4, 5


def next_weekday(day, weekday):
    if day == None:
        day = Date.today()
    delta = (weekday - day.weekday()) % 7
    if delta == 0:
        delta = 7
    return day + delta * ONE_DAY


def prev_weekday(day, weekday):
    if day == None:
        day = Date.today()
    delta = (day.weekday() - weekday) % 7
    if delta == 0:
        delta = 7
    return day - delta * ONE_DAY


def last_weekday_of_month(day, weekday):
    year, month = day.year, day.month
    month += 1
    if month > 12:
        year += 1
        month = 1
    month = Date(year, month, 1)
    return prev_weekday(month, weekday)


def first_weekday_of_month(day, weekday):
    month = Date(day.year, day.month, 1)
    if month.weekday() != weekday():
        return next_weekday(month, weekday)
    else:
        return month




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
