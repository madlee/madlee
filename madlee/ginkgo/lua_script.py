from ..misc.lua import LUA_TS_TO_TIME, upload_scripts
from .const import GINKGO_SEPERATOR, KEY_DAEMON, KEY_YEAR_TS
from .const import KEY_LEAVES, SHA_NEW_LEAF, CMD_NEW_LEAF
from .const import SHA_PUSH, SHA_MISSING



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


LUA_MISSING_SLOTS = '''

%(TS_TO_TIME)s

local dbname = ARGV[1]
local leaf   = ARGV[2]
local start  = ARGV[3]
local finish = ARGV[4]

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
slot = slot.sub(slot, 1, string.find(slot, '%(sep)s')-1)

local key_ts = dbname .. '%(sep)s' .. '%(year_ts)s'

start = ts_to_slot(key_ts, start, slot)
finish = ts_to_slot(key_ts, finish, slot)

local all_slots = list_slots(key_ts, start, finish, slot)
local key_slots = dbname .. '%(sep)s' .. leaf .. '%(sep)s*' 
local existed_keys = redis.call('KEYS', key_slots)
local existed_slots = {}
for i = 1, #existed_keys do
    local s = existed_keys[i]
    local pos = string.len(s) - string.find(string.reverse(s), '|') + 1
    s = string.sub(s, pos+1)
    existed_slots[s] = true
end

local result = {}

local i = 1
while i <= #all_slots do
    local s = all_slots[i]
    if not existed_slots[s] then
        result[#result+1] = s
        local j = i+1
        while j <= #all_slots do
            s = all_slots[j]
            if existed_slots[s] then
                break
            end
            j = j+1
        end
        result[#result+1] = all_slots[j-1]
        i = j
    end
    i = i+1
end

return result

''' % {
    'TS_TO_TIME': LUA_TS_TO_TIME,
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS
}




ALL_LUA_SCRIPTS = {
    SHA_PUSH:     LUA_PUSH_DATA,
    SHA_NEW_LEAF: LUA_NEW_LEAF,
    SHA_MISSING:  LUA_MISSING_SLOTS
    # 'LOAD': LUA_LOAD_DATA,
    # 'LIST': LUA_LIST_SLOT,
    # 'GET':  LUA_GET_DATA,
}





