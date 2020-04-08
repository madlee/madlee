from ..misc.lua import upload_scripts
from .const import GINKGO_SEPERATOR, KEY_YEAR_TS
from .const import KEY_DIRTY, KEY_LAST_SLOT
from .const import SHA_CLEAN_UP, SHA_PUSH, SHA_RESET_DIRTY
from .const import SHA_BRANCH_SIZE


LUA_CLEAN_UP = '''
local pattern = ARGV[1] .. '%(sep)s' .. '*'
local keys = redis.call('KEYS', pattern)
for i = 1, #keys do 
    redis.del(keys[i])
end 

return keys

''' 


LUA_PUSH_DATA = '''
-- Push new data into redis
%(FUNC_TS_TO_SLOT)s

local dbname = ARGV[1]
local branch = ARGV[2]
local key_ts = dbname .. '%(sep)s' .. '%(KEY_YEAR_TS)s'
local key_dirty = dbname .. '%(sep)s' .. '%(KEY_DIRTY)s'

local max_slot = '0'
for i = 3, #ARGV do
    local data = ARGV[i]
    local ts = struct.unpack('d', string.sub(data, 1, 8))
    local slot = ts_to_slot(key_ts, branch, ts, slot)
    if slot > max_slot then
        max_slot = ts
    end
    local key = dbname .. '%(sep)s' .. branch .. '%(sep)s' .. ts
    redis.call('RPUSH', key, data)
    redis.call('EXPIRE', key, %(expire_seconds)d)
    redis.hset('SADD', key_dirty, key)
end
local key = dbname .. '%(sep)s' .. '%(KEY_LAST_SLOT)s'
redis.call('HSET', key, branch, max_slot)

''' 


LUA_RESET_DIRTY = '''
local dbname = ARGV[1]
local key_dirty = dbname .. '%(sep)s' .. '%(KEY_DIRTY)s'
local blocks = redis.call('SMEMBERS', key_dirty)
redis.call('DEL', key_dirty)
return blocks
'''

LUA_BRANCH_SIZE = '''
%(FUNC_BRANCH_SIZE)

local branch = ARGV[1]
return branch_size(branch)

'''


LUA_GET_LAST = '''
local dbname = ARGV[1]
local branch = ARGV[2]
local key = dbname .. '%(sep)s' .. '%(KEY_LAST_SLOT)s'
local slot = redis.call('HGET', key, branch)
return redis.call('LINDEX', slot, -1)
'''


ALL_LUA_SCRIPTS = {
    SHA_CLEAN_UP:       LUA_CLEAN_UP,
    SHA_PUSH:           LUA_PUSH_DATA,
    SHA_RESET_DIRTY:    LUA_RESET_DIRTY,
    SHA_BRANCH_SIZE:    LUA_BRANCH_SIZE,
}


def get_scripts(functions, expire_seconds):
    vars = {
        'sep': GINKGO_SEPERATOR,

        'FUNC_TS_TO_SLOT': functions['FUNC_TS_TO_SLOT'],
        'FUNC_BRANCH_SIZE': functions['FUNC_BRANCH_SIZE'],

        'KEY_LAST_SLOT': KEY_LAST_SLOT,
        'KEY_DIRTY': KEY_DIRTY,
        
        'expire_seconds': expire_seconds
    }

    return {k: v % vars for k, v in ALL_LUA_SCRIPTS.items()}

