from struct import unpack
from .lua_script import ALL_LUA_SCRIPTS
from .base import to_slot, DateTime
from .base import DEFAULT_YEAR_RANGE, SLOT_TS_SIZE, SECONDS_IN_A_DAY
from .backend import connect_backend
from .const import GINKGO_SEPERATOR, KEY_SCRIPTS, KEY_YEAR_TS, KEY_LEAVES


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




class Ginkgo:
    '''A timeseries Database based on redis'''

    def __init__(self, redis, name, readonly=True, style=None):
        self.__name    = name
        self.__redis   = redis
        self.__backend = connect_backend(name, readonly, style)

        scripts = redis.hgetall('%s|%s' % (name, KEY_SCRIPTS))
        if not scripts:
            scripts = self.prepare_redis()
        self.__sha = scripts


    def prepare_redis(self, year_range=DEFAULT_YEAR_RANGE):
        year_range = {year: DateTime(year, 1, 1).timestamp()
            for year in range(year_range[0], year_range[1]+1)
        }
        redis = self.__redis
        redis.zadd(GINKGO_SEPERATOR.join((self.__name, KEY_YEAR_TS)), year_range)

        scripts = {k: redis.script_load(v) for k, v in ALL_LUA_SCRIPTS.items()}
        redis.hmset(GINKGO_SEPERATOR.join((self.__name, KEY_SCRIPTS)), scripts)

        key_leaves = GINKGO_SEPERATOR.join((self.__name, KEY_LEAVES))
        for key, (slot, size) in self.__backend.all_leaves.items():
            self.__redis.hsetnx(key_leaves, key, 
                GINKGO_SEPERATOR.join((str(slot), str(size)))
            )

        return scripts


    @property
    def name(self):
        return self.__name


    @property
    def redis(self):
        return self.__redis


    @property
    def sha(self):
        return self.__sha


    def add_leaf(self, key, slot, size=0):
        added = self.__redis.hget(GINKGO_SEPERATOR.join((self.__name, KEY_LEAVES)), key)
        assert added == GINKGO_SEPERATOR.join((str(slot), str(size)))
        self.__backend.add_leaf(key, slot, size)
        return GinkgoLeaf(key, slot, size)


    def get_leaf(self, key):
        slot = self.__redis.hget(self.__name+'LEAVES', key)
        if not slot:
            raise GinkgoError('Leaf [%s] is NOT exist.')
        slot, size = slot.split('|')
        return GinkgoLeaf(key, slot, int(size))


    def push(self, key, *data):
        return self.__redis.evalsha(self.__luasha_push, 2, self.__name, key, *data)


    def reload(self, key, ts1, ts2):
        '''Load data from backend to redis between ts1 ~ ts2'''
        blocks = self.__backend.load_blocks(key, ts1, ts2)
        self.__redis.evalsha(self.__luasha_load, self.__name, key, *blocks)


    def ensure_get(self, key, ts1, ts2):
        '''Get data between ts1 ~ ts2. Reload the blocks if they are not existed'''
        # all_slots()


