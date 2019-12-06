from .const import GINKGO_SEPERATOR, KEY_DAEMON
from .const import CMD_NEW_LEAF
from .const import KEY_SCRIPTS, SHA_PUSH, SHA_NEW_LEAF




class SyncClient:
    def __init__(self, redis, dbname):
        self.__redis = redis
        self.__dbname = dbname
        sha = redis.hgetall('%s|%s'%(dbname, KEY_SCRIPTS))
        sha = {k.decode():v.decode() for k, v in sha.items()}
        self.__sha_push = sha[SHA_PUSH]
        self.__sha_new_leaf = sha[SHA_NEW_LEAF]


    def add_leaf(self, key, slot, size):
        self.__redis.evalsha(self.__sha_new_leaf, 0, self.__dbname, key, slot, size)


    def push_data(self, key, *data):
        self.__redis.evalsha(self.__sha_push, 0, self.__dbname, key, *data)



