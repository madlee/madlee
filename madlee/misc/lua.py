LUA_BISECT = '''
local bisect_left = function(key, value, start, finish)
    if not start then
        start = 1
    end
    if not finish then
        finish = redis.call('LLEN', key)+1
    end

    while start < finish do
        local mid = (start+finish)/2
        mid = mid - mid%1
        local data = redis.call('LINDEX', key, mid)
        local mid_value = struct.unpack('d', string.sub(data, 1, 8))
        if mid_value < value then
            start = mid+1
        else
            finish = mid
        end
    end
    return start
end


local bisect_right = function(key, value, start, finish)
    if not start then
        start = 1
    end
    if not finish then
        finish = redis.call('LLEN', key)+1
    end

    while start < finish do
        local mid = (start+finish)/2
        mid = mid - mid%1
        local data = redis.call('LINDEX', key, mid)
        local mid_value = struct.unpack('d', string.sub(data, 1, 8))
        if value < mid_value then
            finish = mid
        else
            start = mid+1
        end
    end
    return start
end

'''


LUA_TS_TO_TIME = '''

local SECONDS_IN_AN_HOUR = 60*60
local SECONDS_IN_A_DAY   = 24*SECONDS_IN_AN_HOUR

local ts_to_slot = function(key_year_ts, ts, slot)
    local year_ts = redis.call('ZRANGEBYSCORE', key_year_ts, ts-366*SECONDS_IN_A_DAY, ts, 'WITHSCORES')
    if slot == 'Y' then
        return year_ts[#year_ts-1]
    end 

    local seconds_remains = ts - tonumber(year_ts[#year_ts])
    local year = tonumber(year_ts[#year_ts-1])
    local days = seconds_remains / SECONDS_IN_A_DAY
    local month, day
    local days_in_month = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
    if year%400 == 0 or year%100 ~= 0 and year%4 == 0 then
        days_in_month[2] = 29
    end
    for i = 1, #days_in_month do
        local next_seconds_remains = seconds_remains - days_in_month[i]*SECONDS_IN_A_DAY
        if next_seconds_remains < 0 then
            month = i
            break
        end
        seconds_remains = next_seconds_remains
    end
    
    month = string.format('%04d%02d', year, month)
    if slot == 'm' then
        return month
    end

    local day = seconds_remains / SECONDS_IN_A_DAY
    day = day - day % 1
    seconds_remains = seconds_remains - day * SECONDS_IN_A_DAY
    day = day + 1
    day = month .. string.format('%02d', day)
    if slot == 'd' then
        return day
    end
    if slot then 
        seconds_remains = seconds_remains / slot
        seconds_remains = (seconds_remains - seconds_remains % 1) * slot
    end

    local hour = seconds_remains / SECONDS_IN_AN_HOUR
    hour = hour - hour % 1
    seconds_remains = seconds_remains - hour * SECONDS_IN_AN_HOUR
    local minute = seconds_remains / 60
    minute = minute - minute % 1
    local second = seconds_remains - minute*60
    local result
    if slot then 
        result = string.format('%02d%02d%02d', hour, minute, second)
    else 
        result = string.format('%02d%02d%09.6f', hour, minute, second)
    end
    return day .. result
end 

'''



LUA_CODE_SECTIONS = {
    'BISECT': LUA_BISECT, 
    'TS_TO_TIME': LUA_TS_TO_TIME
}


def upload_scripts(redis, key, **scripts):
    '''Upload scripts and save the SHA to an hash map'''
    sha = {k: redis.script_load(v) for k, v in scripts.items()}
    if key:
        redis.hmset(key, sha)
    return sha
