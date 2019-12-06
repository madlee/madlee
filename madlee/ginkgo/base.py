from abc import ABC
from struct import pack, unpack

from ..misc.time import DateTime, TimeDelta
from ..misc.time import ONE_DAY, SECONDS_IN_AN_HOUR, SECONDS_IN_A_DAY


SLOT_TS_SIZE = {
    'Y': 4,
    'm': 6,
    'd': 8
}

DEFAULT_YEAR_RANGE = [2000, 2100]




def to_slot(slot, dt=None, ts=None):
    if dt is None and ts is None:
        dt = DateTime.now()
    elif dt is None:
        dt = DateTime.fromtimestamp(ts)
    else:
        assert ts is None

    if slot == 'Y':
        return dt.year
    elif slot == 'm':
        return dt.year*100+dt.month
    elif slot == 'd':
        return (dt.year*100+dt.month)*100+dt.day
    else:
        assert SECONDS_IN_A_DAY % slot == 0
        ts = dt.timestamp() // slot * slot
        dt = DateTime.fromtimestamp(ts)
        return int(dt.strftime('%Y%m%d%H%M%S'))


def list_slot(slot, start, finish):
    assert start <= finish
    if slot == 'Y':
        for y in range(start, finish+1):
            yield y
    elif slot == 'm':
        y_s, m_s = start // 100, start % 100
        y_f, m_f = finish // 100, finish % 100
        if y_s == y_f:
            for m in range(m_s, m_f+1):
                yield y_s*100+m
        else:
            for m in range(m_s, 13):
                yield y_s*100+m
            for y in range(y_s, y_f):
                for m in range(1, 13):
                    yield y*100+m
            for m in range(1, m_f+1):
                yield y_f*100+m
    elif slot == 'd':
        start = DateTime.strptime('%Y%m%d', str(start))
        finish = DateTime.strptime('%Y%m%d', str(finish))
        while start <= finish:
            yield int(start.strftime('%Y%m%d'))
            start += ONE_DAY
    else:
        start = DateTime.strptime('%Y%m%d', str(start))
        finish = DateTime.strptime('%Y%m%d', str(finish))
        delta = TimeDelta(seconds=slot)
        while start <= finish:
            yield int(start.strftime('%Y%m%d%H%M%S'))
            start += delta






    
