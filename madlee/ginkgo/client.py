from uuid import uuid4

from .const import GINKGO_SEPERATOR, KEY_DAEMON, KEY_SCRIPTS
from .const import SHA_PUSH, SHA_NEW_LEAF, SHA_MISSING
from .const import SHA_JOIN, SHA_JOIN_SUB, SHA_GET_LAST




class SyncClient:
    def __init__(self, redis, dbname):
        self.__redis = redis
        self.__dbname = dbname
        sha = redis.hmget(
            GINKGO_SEPERATOR.join([dbname, KEY_SCRIPTS]), 
            SHA_PUSH, SHA_GET_LAST
        )
        self.__sha_push     = sha[0]
        self.__sha_get_last = sha[1]


    def push(self, key, *data):
        return self.__redis.evalsha(self.__sha_push, 0, self.__dbname, key, *data)


    def get_last(self, branch, struct=None):
        result = self.__redis.evalsha(self.__sha_get_last, 0, self.__dbname, branch)
        if struct and result:
            result = struct.from_buffer_copy(result)
        return result


class AsyncClient:
    def __init__(self, redis, dbname):
        self.__redis = redis
        self.__dbname = dbname


    async def prepare(self):
        sha = await self.__redis.hmget(
            GINKGO_SEPERATOR.join([self.__dbname, KEY_SCRIPTS]), 
            SHA_PUSH, SHA_GET_LAST
        )
        self.__sha_push     = sha[0]
        self.__sha_get_last = sha[1]



    async def push(self, key, *data):
        result = await self.__redis.evalsha(self.__sha_push, [], [self.__dbname, key] + data)
        return result


    async def get_last(self, branch, struct=None):
        # result = await self.__redis.evalsha(self.__sha_get_last, 0, self.__dbname, branch)
        result = await self.__redis.evalsha(self.__sha_get_last, [], [self.__dbname, branch])
        if struct and result:
            result = struct.from_buffer_copy(result)
        return result
