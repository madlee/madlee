# Please import all in settings.py.


REDIS_KEY_ALERT = 'ALERT'


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
    redis.call('ZREM', basekey .. '|ZO', key)
else
    redis.call('HSET', basekey, key, level .. status .. note)
    redis.call('HSET', basekey .. '|PK', key, pk)
    local key_zo = basekey .. '|ZO'
    if redis.call('ZRANK', key_zo, key) then
    else
        redis.call('ZADD', basekey .. '|ZO', 0, key)
    end
end

'''


