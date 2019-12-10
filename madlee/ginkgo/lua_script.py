from ..misc.lua import upload_scripts
from .const import GINKGO_SEPERATOR, KEY_DAEMON, KEY_YEAR_TS, KEY_SCRIPTS
from .const import KEY_LEAVES, SHA_NEW_LEAF, CMD_NEW_LEAF
from .const import SHA_PUSH, SHA_MISSING, SHA_LOAD, SHA_JOIN, SHA_AUTO_LEAF
from .const import SHA_ALL_SLOTS, SHA_NEWER_SLOTS

try:
    from conf.ginkgo import LUA_FUNCTION_SET 
except ImportError:
    from .lua_function import LUA_FUNCTION_SET


LUA_NEW_LEAF = '''
%(FUNC_NEW_LEAF)s

new_leaf(ARGV[1], ARGV[2], ARGV[3], ARGV[4])

''' % LUA_FUNCTION_SET



LUA_PUSH_DATA = '''
-- Push new data into redis
%(FUNC_TS_TO_TIME)s
%(FUNC_AUTO_LEAF)s

local dbname = ARGV[1]
local leaf = ARGV[2]
local key_ts = dbname .. '%(sep)s' .. '%(year_ts)s'

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
if slot then 
    slot = string.sub(slot, 1, string.find(slot, '%(sep)s')-1)
else
    slot = auto_leaf(dbname, leaf)
end

for i = 3, #ARGV do
    local data = ARGV[i]
    local ts = struct.unpack('d', string.sub(data, 1, 8))
    ts = ts_to_slot(key_ts, ts, slot)
    local key = dbname .. '%(sep)s' .. leaf .. '%(sep)s' .. ts
    redis.call('RPUSH', key, data)
end

''' % {
    'FUNC_TS_TO_TIME': LUA_FUNCTION_SET['FUNC_TS_TO_TIME'],
    'FUNC_AUTO_LEAF': LUA_FUNCTION_SET['FUNC_AUTO_LEAF'],
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS,
    'scripts': KEY_SCRIPTS,
    'auto_leaf': SHA_AUTO_LEAF
}



LUA_LIST_ALL_SLOTS = '''
%(FUNC_JOIN_BLOCK)s

local dbname = ARGV[1]
local leaf   = ARGV[2]

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
local size = tonumber(slot.sub(slot, string.find(slot, '%(sep)s')+1))

local key_slots = dbname .. '%(sep)s' .. leaf .. '%(sep)s*' 
local existed_keys = redis.call('KEYS', key_slots)

local result = {}
for i = 1, #existed_keys do 
    local key = existed_keys[i]
    local pos = string.len(key) - string.find(string.reverse(key), '|') + 1
    local slot_s = string.sub(key, pos+1)
    result[#result+1] = slot_s
    local data = join_block(key, size)
    result[#result+1] = data[1]
    result[#result+1] = data[2]
    result[#result+1] = data[3]
    result[#result+1] = data[4]
end

return result
''' % {
    'FUNC_JOIN_BLOCK': LUA_FUNCTION_SET['FUNC_JOIN_BLOCK'],
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
}


LUA_LIST_NEWER_SLOTS = '''
%(FUNC_JOIN_BLOCK)s

local dbname = ARGV[1]
local leaf   = ARGV[2]
local last_slot = ARGV[3]

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
local size = tonumber(slot.sub(slot, string.find(slot, '%(sep)s')+1))

local key_slots = dbname .. '%(sep)s' .. leaf .. '%(sep)s*' 
local existed_keys = redis.call('KEYS', key_slots)

local result = {}
for i = 1, #existed_keys do 
    local key = existed_keys[i]
    local pos = string.len(key) - string.find(string.reverse(key), '|') + 1
    local slot_s = string.sub(key, pos+1)
    if slot_s >= last_slot then
        result[#result+1] = slot_s
        local data = join_block(key, size)
        result[#result+1] = data[1]
        result[#result+1] = data[2]
        result[#result+1] = data[3]
        result[#result+1] = data[4]
    end
end

return result
''' % {
    'FUNC_JOIN_BLOCK': LUA_FUNCTION_SET['FUNC_JOIN_BLOCK'],
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
}


LUA_MISSING_SLOTS = '''

%(FUNC_TS_TO_TIME)s

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
    'FUNC_TS_TO_TIME': LUA_FUNCTION_SET['FUNC_TS_TO_TIME'],
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS
}


LUA_LOAD_DATA = '''
-- Load block data into redis
%(FUNC_TS_TO_TIME)s

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
    'FUNC_TS_TO_TIME': LUA_FUNCTION_SET['FUNC_TS_TO_TIME'],
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS
}


LUA_GET_SLOT = '''

local key       = KEYS[0]
local size      = ARGV[1]


'''

LUA_JOIN_DATA = '''
%(FUNC_TS_TO_TIME)s
%(FUNC_JOIN_BLOCK)s

local dbname  = ARGV[1]
local start   = tonumber(ARGV[2])
local finish  = tonumber(ARGV[3])
local leaf    = ARGV[4]

local key_ts = dbname .. '%(sep)s' .. '%(year_ts)s'

local slot = redis.call('HGET', dbname .. '%(sep)s' .. '%(leaves)s', leaf)
local slotsize = tonumber(slot.sub(slot, string.find(slot, '%(sep)s')+1))
slot = slot.sub(slot, string.find(slot, '%(sep)s')+1)

start = ts_to_slot(key_ts, start, slot)
finish = ts_to_slot(key_ts, finish, slot)
local all_slots = list_slots(key_ts, start, finish, slot)

local result = {}
for i = 4, #ARGV do 
    for j = 1, #all_slots do
        local key = dbname .. '%(sep)s' .. leaf .. '%(sep)s' .. all_slots[j]
        local data = join_block(key, slotsize)
        result[#result+1] = data
    end
end

return result

''' % {
    'FUNC_TS_TO_TIME': LUA_FUNCTION_SET['FUNC_TS_TO_TIME'],
    'FUNC_JOIN_BLOCK': LUA_FUNCTION_SET['FUNC_JOIN_BLOCK'],
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'year_ts': KEY_YEAR_TS
}




LUA_JOIN_SUB = '''
%(FUNC_TS_TO_TIME)s

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
    'FUNC_TS_TO_TIME': LUA_FUNCTION_SET['FUNC_TS_TO_TIME'],
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

    SHA_ALL_SLOTS:   LUA_LIST_ALL_SLOTS,
    SHA_NEWER_SLOTS: LUA_LIST_NEWER_SLOTS,

    # 'LIST': LUA_LIST_SLOT,
    # 'GET':  LUA_GET_DATA,
}



