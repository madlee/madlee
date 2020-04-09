from struct import pack, unpack

from madlee.misc.lua import upload_scripts
from madlee.misc.time import DateTime
from .backend import connect_backend
from .const import KEY_YEAR_TS, DEFAULT_YEAR_RANGE
from .const import GINKGO_SEPERATOR, KEY_SCRIPTS
from .const import SHA_CLEAN, SHA_RESET_DIRTY
from .const import SHA_BRANCH_SIZE




class GinkgoDaemon:
    def __init__(self, redis, name, style=None, func_branch_size=None):
        self.__redis = redis
        self.__name  = name
        self.__backend = connect_backend(name, False, style)
        key = GINKGO_SEPERATOR.join([self.name, KEY_SCRIPTS])
        self.__sha = redis.hgetall(key)

        if func_branch_size:
            self.branch_size = func_branch_size



    def reset_redis(self, scripts):
        redis = self.__redis
        clean_up = scripts[SHA_CLEAN]
        redis.eval(clean_up, 0, self.name) # Script Clean up

        year_range = {year: DateTime(year, 1, 1).timestamp()
            for year in range(DEFAULT_YEAR_RANGE[0], DEFAULT_YEAR_RANGE[1]+1)
        }
        redis.zadd(GINKGO_SEPERATOR.join((self.__name, KEY_YEAR_TS)), year_range)

        key = GINKGO_SEPERATOR.join([self.name, KEY_SCRIPTS])
        self.__sha = upload_scripts(redis, key, **scripts)


    @property
    def name(self):
        return self.__name


    @property
    def redis(self):
        return self.__redis


    def branch_size(self, branch):
        return self.redis.evalsha(self.__sha[SHA_BRANCH_SIZE], 0, branch)


    def pack(self, data, size):
        if size:
            return b''.join(data)
        else:
            n = len(data)
            header = pack('L'*(n+1), *([n] + [len(v) for v in data]))
            return header + b''.join(data)


    def unpack(self, data, size):
        if size:
            return [data[i:i+size] for i in range(0, len(data), size)]
        else:
            n = unpack('L', data[:4])[0]
            length = unpack('L'*n, data[4:(n+1)*4])
            data = data[4*(n+1):]
            result = []
            start = 0
            for row in length:
                result.append(data[start:start+row])
                start += row
            return result


    def save_dirty(self):
        dirties = self.redis.evalsha(
            self.__sha[SHA_RESET_DIRTY], 0, self.name
        )
        if dirties:
            dirties = {
                row.decode(): self.redis.lrange(row, 0, -1)
                for row in dirties
            }
            result = list(dirties.keys())
            blocks = {}
            for row, block in dirties.items():
                _, branch, slot = row.split(GINKGO_SEPERATOR)
                slot = int(slot)
                if branch in blocks:
                    blocks[branch][slot] = block
                else:
                    blocks[branch] = {slot: block}

            for branch, row in blocks.items():
                size = self.branch_size(branch)
                row = [(slot, self.pack(v, size)) for slot, v in row.items()]
                self.__backend.save(branch, *row)
        else:
            result = []
        
        return result
