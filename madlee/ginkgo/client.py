from uuid import uuid4

from .const import GINKGO_SEPERATOR, KEY_DAEMON
from .const import CMD_NEW_LEAF
from .const import KEY_SCRIPTS, SHA_PUSH, SHA_NEW_LEAF, SHA_MISSING
from .const import SHA_JOIN, SHA_JOIN_SUB, SHA_GET_LAST, KEY_LEAVES
from .const import CMD_ENSURE




class SyncClient:
    def __init__(self, redis, dbname):
        self.__redis = redis
        self.__dbname = dbname
        sha = redis.hmget(
            GINKGO_SEPERATOR.join([dbname, KEY_SCRIPTS]), 
            SHA_PUSH, SHA_GET_LAST
        )
        sha = {k.decode():v.decode() for k, v in sha.items()}
        self.__sha_push     = sha[SHA_PUSH]
        self.__sha_get_last = sha[SHA_GET_LAST]


    def push(self, key, *data):
        return self.__redis.evalsha(self.__sha_push, 0, self.__dbname, key, *data)


    def get_last(self, branch, struct=None):
        result = self.__redis.evalsha(self.__sha_get_last, 0, self.__dbname, branch)
        if struct and result:
            result = struct.from_buffer_copy(result)
        return result
