from abc import ABC
from struct import pack, unpack

from ..misc.time import DateTime, TimeDelta
from ..misc.time import ONE_DAY, SECONDS_IN_AN_HOUR, SECONDS_IN_A_DAY
from ..misc.lua import LUA_TS_TO_TIME, upload_scripts
from .backend import connect_backend


DEFAULT_YEAR_RANGE = [2000, 2100]


LUA_PUSH_DATA = '''
%(TS_TO_TIME)s

local prefix = KEYS[1]
local key_base = KEYS[2]

local slot = redis.call('HGET', prefix .. '|LEAVES', key_base)
slot = slot.sub(slot, 1, string.find(slot, '|'))

local i
for i = 1, #ARGV do
    local data = ARGV[i]
    local ts = ts_to_date(prefix, data, slot)
    local key = perfix .. '|' .. key_base .. '|' .. ts
    redis.call('RPUSH', key, data)
end

''' % {'TS_TO_TIME': LUA_TS_TO_TIME}

LUA_LOAD_DATA = '''
%(TS_TO_TIME)s

local prefix = KEYS[1]
local key_base = KEYS[2]
local slot = redis.call('HGET', prefix .. '|LEAVES', key_base)
local size = 0+slot.sub(slot, string.find(slot, '|')+1)
slot = slot.sub(slot, 1, string.find(slot, '|'))

if slot == 0 then
    # TODO: 
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
    'PUSH': LUA_PUSH_DATA
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
    


class GinkgoError(RuntimeError):
    pass


class GinkgoLeaf:
    def __init__(self, key, slot, size):
        try:
            self.__ts_size = SLOT_TS_SIZE[slot]
            self.__slot    = slot
        except KeyError:
            slot = int(slot)
            assert SECONDS_IN_A_DAY % slot == 0
            self.__ts_size = 0
            self.__slot    = int(slot)
        
        self.__key      = key+'|'
        self.__size     = size


    def key(self, data):
        ts = unpack('d', data[:8])[0]
        return self.__key + to_slot(self.__slot, ts=ts)


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




class Ginkgo:
    '''A timeseries Database based on redis'''

    def __init__(self, redis, prefix, readonly=True, style=None):
        self.__prefix       = prefix
        self.__redis        = redis
        self.__backend = connect_backend(prefix, readonly, style)

        scripts = redis.hgetall(prefix+'|SCRIPTS')
        if scripts is None:
            scripts = self.prepare_redis()
        self.__luasha_push  = scripts['PUSH']
        self.__luasha_load  = scripts['LOAD']


    def prepare_redis(self, year_range=DEFAULT_YEAR_RANGE):
        year_range = {year: DateTime(year, 1, 1).timestamp()
            for year in range(year_range[0], year_range[1]+1)
        }
        redis = self.__redis
        redis.zadd(self.__prefix+'|YEAR_START_TS', year_range)

        scripts = {k: redis.script_load(v) for k, v in ALL_LUA_SCRIPTS.items()}
        redis.hmset(self.__prefix+'|SCRIPTS', scripts)

        for key, (slot, size) in self.__backend.all_leaves.items():
            self.__redis.hsetnx(self.__prefix+'LEAVES', key, '%s|%s' % (slot, size))

        return scripts


    @property
    def prefix(self):
        return self.__prefix


    @property
    def redis(self):
        return self.__redis


    def add_leaf(self, key, slot, size=0):
        assert slot in SLOT_CHOICES
        added = self.__redis.hsetnx(self.__prefix+'LEAVES', key, '%s|%s' % (slot, size))
        if not added:
            raise GinkgoError('Leaf [%s] has existed.')
        self.__backend.add_leaf(key, slot, size)
        return GinkgoLeaf(key, slot, size)


    def get_leaf(self, key):
        slot = self.__redis.hget(self.__prefix+'LEAVES', key)
        if not slot:
            raise GinkgoError('Leaf [%s] is NOT exist.')
        slot, size = slot.split('|')
        return GinkgoLeaf(key, slot, int(size))


    def push(self, key, *data):
        return self.__redis.evalsha(self.__luasha_push, 2, self.__prefix, key, *data)


    def reload(self, key, ts1, ts2):
        '''Load data from backend to redis between ts1 ~ ts2'''
        blocks = self.__backend.load_blocks(key, ts1, ts2)
        self.__redis.evalsha(self.__luasha_load, self.__prefix, key, *blocks)


    def ensure_get(self, key, ts1, ts2):
        '''Get data between ts1 ~ ts2. Reload the blocks if they are not existed'''




    
