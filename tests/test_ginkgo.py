from time import time as now, sleep
from struct import pack
from random import uniform
from django_redis import get_redis_connection

from madlee import prepare
from madlee.ginkgo import SyncClient as GinkgoClient



if __name__ == '__main__':
    redis = get_redis_connection('ginkgo')
    ginkgo = GinkgoClient(redis, 'TEST')
    for _ in range(100):
        n, v = now(), uniform(0, 10)
        data = pack('dd', n, v)
        ginkgo.push('MILIMALA', data)
        print (n, v)
        sleep(v)

    