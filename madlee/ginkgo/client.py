



class Client:
    def __init__(self, dbname, redis):
        self.__redis = redis
        self.__dbname = dbname
        self.__scripts = sha = redis.hgetall(dbname+'GINKGO-SCRIPTS')
        self.__call_sha = sha['CALL']


    def add_leaf(self, leafname, slot, size):
        self.__redis.evalsha(self.__call_sha, 1, self.__dbname, leafname, slot, size)

        

    