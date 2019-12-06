from ..misc.lua import LUA_TS_TO_TIME, upload_scripts
from .const import GINKGO_SEPERATOR, KEY_DAEMON, KEY_YEAR_TS
from .const import KEY_LEAVES, SHA_NEW_LEAF, CMD_NEW_LEAF
from .const import SHA_PUSH



LUA_NEW_LEAF = '''
local key = ARGV[1] .. '%(sep)s' .. '%(leaves)s'
local leafname = ARGV[2]

local data = tostring(ARGV[3]) .. '%(sep)s' .. tostring(ARGV[4])

if redis.call('HSETNX', key, leafname, data) then
    key = ARGV[1] .. '%(sep)s' .. '%(daemon)s' .. '%(sep)s' .. '%(new_leaf)s'
    data = leafname .. '%(sep)s' .. data
    redis.call('PUBLISH', key, data)
end

''' % {
    'sep': GINKGO_SEPERATOR, 
    'daemon': KEY_DAEMON,
    'leaves': KEY_LEAVES, 
    'new_leaf': CMD_NEW_LEAF
}



LUA_PUSH_DATA = '''
-- Push new data into redis
%(TS_TO_TIME)s

local dbname = ARGV[1]
local leaf = ARGV[2]
local key_ts = dbname .. '%(sep)s' .. '%(year_ts)s'

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
slot = slot.sub(slot, 1, string.find(slot, '%(sep)s')-1)

for i = 3, #ARGV do
    local data = ARGV[i]
    local ts = struct.unpack('d', string.sub(data, 1, 8))
    ts = ts_to_slot(key_ts, ts, slot)
    local key = dbname .. '%(sep)s' .. leaf .. '%(sep)s' .. ts
    redis.call('RPUSH', key, data)
end

''' % {
    'TS_TO_TIME': LUA_TS_TO_TIME,
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS
}








ALL_LUA_SCRIPTS = {
    SHA_PUSH: LUA_PUSH_DATA,
    SHA_NEW_LEAF: LUA_NEW_LEAF,
    # 'LOAD': LUA_LOAD_DATA,
    # 'LIST': LUA_LIST_SLOT,
    # 'GET':  LUA_GET_DATA,
}





