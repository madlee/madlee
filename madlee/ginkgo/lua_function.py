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

LUA_FUNCTION_SET = {
    'FUNC_NEW_LEAF':  LUA_FUNC_NEW_LEAF,
    'FUNC_AUTO_LEAF': LUA_FUNC_AUTO_LEAF,
    'FUNC_TS_TO_TIME': LUA_TS_TO_TIME
}

