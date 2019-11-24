from abc import ABC
from struct import pack, unpack

from ..misc.time import DateTime, TimeDelta
from ..misc.time import ONE_DAY, SECONDS_IN_AN_HOUR, SECONDS_IN_A_DAY
from ..misc.lua import LUA_TS_TO_TIME, upload_scripts


DEFAULT_YEAR_RANGE = [2000, 2100]


LUA_PUSH_DATA = '''
-- Push new data into redis and publish message when new block was created..
%(TS_TO_TIME)s

local prefix = KEYS[1]
local leaf = KEYS[2]

local slot = redis.call('HGET', prefix .. '|LEAVES', leaf)
slot = slot.sub(slot, 1, string.find(slot, '|'))

local i
local last_ts = redis.call('HGET', prefix .. '|LAST_SLOT', leaf)
for i = 1, #ARGV do
    local data = ARGV[i]
    local ts = ts_to_slot(prefix, data, slot)
    local key = perfix .. '|' .. leaf .. '|' .. ts
    redis.call('RPUSH', key, data)

    if ts != last_ts then 
        if last_ts then 
            local message = tostring(ts)
            redis.call('PUBLISH', prefix .. '|NEW_SLOT|' .. leaf, message)
        else 
            local message = tostring(last_ts) .. '|' .. tostring(ts)
            redis.call('PUBLISH', prefix .. '|NEW_SLOT|' .. leaf, message)
        end
        reds.call('HSET', prefix .. '|LAST_SLOT', leaf, ts)
        last_ts = ts
    end
end

''' % {'TS_TO_TIME': LUA_TS_TO_TIME}




LUA_LOAD_DATA = '''
-- Load block data into redis and NO other side effects.

%(TS_TO_TIME)s

local prefix = KEYS[1]
local key_base = KEYS[2]
local slot = redis.call('HGET', prefix .. '|LEAVES', key_base)
local size = 0+slot.sub(slot, string.find(slot, '|')+1)
slot = slot.sub(slot, 1, string.find(slot, '|'))

if slot == 0 then
    -- TODO
else
    local i, j
    for i = 1, #ARGV do
        local block = ARGV[i]
        local ts = ts_to_date(prefix, block, slot)
        local key = perfix .. '|' .. key_base .. '|' .. ts
        for j = 1, #block, size do
            redis.call('RPUSH', key, string.sub(block, j, j+size-1))
        end
    end
end

'''





ALL_LUA_SCRIPTS = {
    'PUSH': LUA_PUSH_DATA,
    'LOAD': LUA_LOAD_DATA,
    'LIST': LUA_LIST_SLOT,
    'GET':  LUA_GET_DATA,
}


SLOT_CHOICES = {
    'Y': 'year', 'm': 'month', 'd': 'day'
}

SLOT_TS_SIZE = {
    'Y': 4, 'm': 6, 'd': 8
}


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






    
