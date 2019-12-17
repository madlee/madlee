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
        sha = redis.hgetall('%s|%s'%(dbname, KEY_SCRIPTS))
        sha = {k.decode():v.decode() for k, v in sha.items()}
        self.__sha_push     = sha[SHA_PUSH]
        self.__sha_new_leaf = sha[SHA_NEW_LEAF]
        self.__sha_missing  = sha[SHA_MISSING]
        self.__sha_join     = sha[SHA_JOIN]
        self.__sha_get_last = sha[SHA_GET_LAST]


    @property
    def all_leaves(self):
        key = GINKGO_SEPERATOR.join((self.__dbname, KEY_LEAVES))
        result = {
            k.decode(): v.decode().split(GINKGO_SEPERATOR) 
            for k, v in self.__redis.hgetall(key).items()
        }
        result = {k: ((v[0] if v[0] in 'Ymd' else int(v[0])), int(v[1])) for k, v in result.items()} 
        return result


    def add_leaf(self, key, slot, size):
        self.__redis.evalsha(self.__sha_new_leaf, 0, self.__dbname, key, slot, size)


    def push(self, key, *data):
        return self.__redis.evalsha(self.__sha_push, 0, self.__dbname, key, *data)


    def missing_slots(self, key, start, finish):
        start = start.timestamp()
        finish = finish.timestamp()
        return self.__redis.evalsha(self.__sha_missing, 0, 
            self.__dbname, key, start, finish
        )


    def get_last(self, leaf, struct=None):
        result = self.__redis.evalsha(self.__sha_get_last, 0, self.__dbname, leaf)
        if struct and result:
            result = struct.from_buffer_copy(result)
        return result



    def ensure(self, start, finish, *keys):
        publish_key = GINKGO_SEPERATOR.join((
            self.__dbname, KEY_DAEMON, CMD_ENSURE
        ))
        keys = GINKGO_SEPERATOR.join(keys)
        start = str(start.timestamp())
        finish = str(finish.timestamp())
        call_back = str(uuid4())
        msg = GINKGO_SEPERATOR.join((call_back, start, finish, keys))
        self.__redis.publish(publish_key, msg)


    def get(self, start, finish, *keys):
        start = start.timestamp()
        finish = finish.timestamp()
        return self.__redis.evalsha(self.__sha_join, 0, 
            self.__dbname, start, finish, *keys
        )


