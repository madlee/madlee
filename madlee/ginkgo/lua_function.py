from ..misc.lua import LUA_TS_TO_TIME, upload_scripts
from .const import GINKGO_SEPERATOR, KEY_DAEMON, KEY_YEAR_TS, KEY_SCRIPTS
from .const import KEY_LEAVES, CMD_NEW_LEAF


LUA_FUNC_NEW_LEAF = '''
local new_leaf = function(dbname, leafname, slottype, size)
    local key = dbname .. '%(sep)s' .. '%(leaves)s'
    local data = tostring(slottype) .. '%(sep)s' .. tostring(size)
    if redis.call('HSETNX', key, leafname, data) then
        key = dbname .. '%(sep)s' .. '%(daemon)s' .. '%(sep)s' .. '%(new_leaf)s'
        data = leafname .. '%(sep)s' .. data
        redis.call('PUBLISH', key, data)
    end
end
''' % {
    'sep': GINKGO_SEPERATOR,
    'leaves': KEY_LEAVES,
    'daemon': KEY_DAEMON,
    'new_leaf': CMD_NEW_LEAF
}

LUA_FUNC_AUTO_LEAF = '''
local auto_leaf = function(dbname, leafname)
    error('UNKNOWN LEAF [' .. dbname .. '] ' .. leafname)
end 
'''

LUA_FUNC_JOIN_BLOCK = '''
local join_block = function(key, size)
    local data = redis.call('LRANGE', key, 0, -1)
    local result
    local length = #data
    local start = string.sub(data[1], 1, 8)
    local finish = string.sub(data[#data], 1, 8)

    if size == 0 then
        result = {}
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

    return {length, start, finish, table.concat(result)}
end

'''



LUA_FUNC_JOIN_SUB = '''
local join_sub = function(key, offset, size, withts)
    local data = redis.call('LRANGE', key, 0, -1)
    local result = {}
    for i = 1, #data do
        if withts then
            result[#result+1] = string.sub(data[i], 1, 8)
        end
        result[#result+1] = string.sub(data[i], offset+1, offset+size)
    end
    return table.concat(result)
end
'''


LUA_FUNCTION_SET = {
    'FUNC_NEW_LEAF':  LUA_FUNC_NEW_LEAF,
    'FUNC_AUTO_LEAF': LUA_FUNC_AUTO_LEAF,
    'FUNC_TS_TO_TIME': LUA_TS_TO_TIME,
    'FUNC_JOIN_BLOCK': LUA_FUNC_JOIN_BLOCK,
    'FUNC_JOIN_SUB': LUA_FUNC_JOIN_SUB
}

