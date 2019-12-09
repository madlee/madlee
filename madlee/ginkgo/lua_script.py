from ..misc.lua import LUA_TS_TO_TIME, upload_scripts
from .const import GINKGO_SEPERATOR, KEY_DAEMON, KEY_YEAR_TS, KEY_SCRIPTS
from .const import KEY_LEAVES, SHA_NEW_LEAF, CMD_NEW_LEAF
from .const import SHA_PUSH, SHA_MISSING, SHA_LOAD, SHA_JOIN, SHA_AUTO_LEAF




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
if slot == nil then
    local sha_autoleaf = redis.call('HGET', dbname .. '%(sep)s' .. '%(scripts)s', '%(auto_leaf)s')
    if sha_autoleaf then
        slot = redis.call('EVALSHA', sha_autoleaf, 0, leaf)[1]
    end
else
    slot = slot.sub(slot, 1, string.find(slot, '%(sep)s')-1)
end

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
    'year_ts': KEY_YEAR_TS,
    'scripts': KEY_SCRIPTS,
    'auto_leaf': SHA_AUTO_LEAF
}


print (LUA_PUSH_DATA)


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


LUA_LOAD_DATA = '''
-- Load block data into redis
%(TS_TO_TIME)s

local dbname = ARGV[1]
local leaf   = ARGV[2]

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
local slotsize = tonumber(slot.sub(slot, string.find(slot, '%(sep)s')+1))
slot = slot.sub(slot, string.find(slot, '%(sep)s')+1)

local result = 0

for i = 3, #ARGV do
    local block = ARGV[i]
    local ts = struct.unpack('d', string.sub(block, 1, 8))
    local key = dbname .. '%(sep)s' .. '%(leaves)s' .. '%(sep)s' .. ts_to_slot(key_ts, ts, slot)
    local data = {}
    for j = 1, string.len(block), slotsize do
        data[#data] = string.sub(block, j, j+slotsize-1)
    end
    redis.call('RPUSH', key, unpack(data))
    result = result + #data
end

return result

''' % {
    'TS_TO_TIME': LUA_TS_TO_TIME,
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS
}


LUA_GET_SLOT = '''

local key       = KEYS[0]
local size      = ARGV[1]

local data = redis.call('LRANGE', key, 0, -1)

if size == 0 then
    local result = {}
    result[1] = struct.pack('l', #data)
    local offset = 0
    for i = 1, #data do
        offset = offset + string.len(data[i])
        result[#result+1] = struct.pack('l', offset)
    end
    for i = 1, #data do
        result[#result+1] = data[i]
    end
else
    result = data
end 

return table.concat(result)

'''

LUA_JOIN_DATA = '''
%(TS_TO_TIME)s

local dbname  = ARGV[1]
local start   = tonumber(ARGV[2])
local finish  = tonumber(ARGV[3])
local leaf    = ARGV[4]

local key_ts = dbname .. '%(sep)s' .. '%(year_ts)s'

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
slot = slot.sub(slot, string.find(slot, '%(sep)s')+1)

start = ts_to_slot(key_ts, start, slot)
finish = ts_to_slot(key_ts, finish, slot)
local all_slots = list_slots(key_ts, start, finish, slot)

local result = {}
for i = 4, #ARGV do 
    for j = 1, #all_slots do
        local key = dbname .. '%(sep)s' .. leaf .. '%(sep)s' .. all_slots[j]
        local data = redis.call('LRANGE', key, 0, -1)
        if data then 
            result[#result+1] = table.concat(data)
        end
    end
end

return result

''' % {
    'TS_TO_TIME': LUA_TS_TO_TIME,
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS
}




LUA_JOIN_SUB = '''
%(TS_TO_TIME)s

local dbname    = ARGV[1]
local start     = tonumber(ARGV[2])
local finish    = tonumber(ARGV[3])
local offset    = tonumber(ARGV[4])
local size      = tonumber(ARGV[5])
local withstamp = tonumber(ARGV[6])
local leaf      = ARGV[7]

local key_ts = dbname .. '%(sep)s' .. '%(year_ts)s'

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
slot = slot.sub(slot, string.find(slot, '%(sep)s')-1)

start = ts_to_slot(key_ts, start, slot)
finish = ts_to_slot(key_ts, finish, slot)
local all_slots = list_slots(key_ts, start, finish, slot)

local result = {}
local keybase = dbname .. '%(sep)s' .. leaf .. '%(sep)s'
for i = 7, #ARGV do 
    for j = 1, #all_slots do
        local key = keybase .. all_slots[j]
        local data = redis.call('LRANGE', key, 0, -1)
        if data then 
            local dd = {}
            for k = 1, #data do
                local s
                if withstamp == 1 then
                    s = string.sub(data[k], 1, 8) 
                else
                    s = ''
                end
                s = s .. string.sub(data[k], offset+1, offset+size)
                dd[#dd+1] = s
            end
            result[#result+1] = table.concat(dd)
        end
    end
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
    SHA_MISSING:  LUA_MISSING_SLOTS,
    SHA_LOAD:     LUA_LOAD_DATA,
    SHA_JOIN:     LUA_JOIN_DATA,

    # 'LIST': LUA_LIST_SLOT,
    # 'GET':  LUA_GET_DATA,
}



