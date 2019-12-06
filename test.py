from time import sleep, time as now
from struct import pack
from redis import StrictRedis as Redis

from madlee.ginkgo import Ginkgo
from madlee.ginkgo.client import SyncClient as GinkgoClient
from madlee.ginkgo.lua_script import LUA_PUSH_DATA



if __name__ == '__main__':
    redis = Redis(db=1)
    db  = Ginkgo(redis, 'TEST')
    db.prepare_redis()
    print (redis.hgetall(b'TEST|GINKGO-SCRIPTS'))
    client = GinkgoClient(redis, 'TEST')
    client.add_leaf('DATA', 60, 64)
    print (redis.keys())


    print ('\n'*4, '$$$$', LUA_PUSH_DATA, '\n'*4)

    for _ in range(100):
        sleep(3)
        t = now()
        data = pack('d', t)
        data += b' '*56
        print (t)
        assert len(data) == 64
        client.push_data('DATA', data)


