# Please import all in settings.py.

GAP_ALERT_CHECK = 5 # Seconds

REDIS_KEY_ALERT = 'ALERT'

FORMAT_ALERT_TEXT = '[%(level)s] %(name)s\n%(note)s'




REDIS_LUA_UPDATE_ALERT = '''
local basekey = KEYS[1]
local pk = ARGV[1]
local key = ARGV[2] .. '|' .. ARGV[3]
local note = ARGV[4]
local level = ARGV[5]
local status = ARGV[6]

if status == 'C' then
    -- Alert closed. Remove all from redis
    redis.call('HDEL', basekey, key)
    redis.call('HDEL', basekey .. '|PK', key)
    redis.call('HDEL', basekey .. '|TS', key)
else
    redis.call('HSET', basekey, key, level .. status .. note)
    redis.call('HSET', basekey .. '|PK', key, pk)
end

'''


REDIS_LUA_GET_ALERT = '''
local GAP_OF_ALERT = {
    'WO': 3600,  'WN': 28800, 
    'DO': 300,   'DN': 3600, 
    'EO': 60,    'EN': 300, 
}

local basekey = KEYS[1]
local now = ARGV[1]

local alert   = redis.call('HGETALL', basekey)
local ts      = redis.call('HGETALL', basekey+'|TS')
local result  = {}, new_ts = {}
local i 
for i = 1, #alert, 2 do
    local key = alert[i]
    local val = alert[i+1]
    local tag = string.sub(val, 1, 2)
    local delta = GAP_OF_ALERT[tag]
    if delta then
        local last_time = ts[key]
        if not last_time then
            last_time = 0
        end
        if last_time + delta < now then
            result[#result+1] = key
            result[#result+1] = val
            new_ts[#new_ts+1] = key
            new_ts[#new_ts+1] = now
        end
    end
end

redis.call('HMSET', unpack(new_ts))

return result
'''


