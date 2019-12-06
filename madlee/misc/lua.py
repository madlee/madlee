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


local days_in_month = function(year) 
    local result = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
    if year%400 == 0 or year%100 ~= 0 and year%4 == 0 then
        result[2] = 29
    end
    return result
end 


local ts_to_slot = function(key_year_ts, ts, slot)
    local year_ts = redis.call('ZRANGEBYSCORE', key_year_ts, ts-366*SECONDS_IN_A_DAY, ts, 'WITHSCORES')
    if slot == 'Y' then
        return year_ts[#year_ts-1]
    end 

    local seconds_remains = ts - tonumber(year_ts[#year_ts])
    local year = tonumber(year_ts[#year_ts-1])
    local days = seconds_remains / SECONDS_IN_A_DAY
    local month, day
    local dim = days_in_month(year)
    for i = 1, #dim do
        local next_seconds_remains = seconds_remains - dim[i]*SECONDS_IN_A_DAY
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


local slot_to_ts = function(key_year_ts, slot)
    local year = string.sub(slot, 1, 4)
    local result = tonumber(redis.call('ZSCORE', key_year_ts, year))
    if string.len(slot) == 4 then
        return result
    end

    year = tonumber(year)
    local month = tonumber(string.sub(slot, 5, 6))
    local dim = days_in_month(year)
    for i = 1, month-1 do
        result = result + SECONDS_IN_A_DAY*dim[i]
    end
    if string.len(slot) == 6 then
        return result
    end

    local day = tonumber(string.sub(slot, 7, 8))
    result = result + SECONDS_IN_A_DAY*(day-1)
    if string.len(slot) == 8 then
        return result
    end

    local hour = tonumber(string.sub(slot, 9, 10))
    result = result + hour * SECONDS_IN_AN_HOUR
    local minute = tonumber(string.sub(slot, 11, 12))
    result = result + minute * 60
    local second = tonumber(string.sub(slot, 13, 14))
    result = result + second
    return result
end


local list_slots = function(key_year_ts, start, finish, slot) 
    -- List slots between [start, finish]
    -- start and finish are slot too.
    start = tonumber(start)
    finish = tonumber(finish)
    local result = {}
    if slot == 'Y' then
        for year = start, finish do
            result[#result+1] = year
        end
    elseif slot == 'm' then
        local start_month = start % 100
        local start_year  = (start - start_month) / 100 
        local finsh_month = finish % 100
        local finish_year = (finish - finsh_month) / 100
        if start_year ~= finish_year then
            for month = start_month, 12 do
                result[#result+1] = start_year*100+month
            end
            for year = start_month+1, finish_year-1 do
                for month = 1, 12 do
                    result[#result+1] = year*100+month
                end
            end
            for month = 1, finish_month do
                result[#result+1] = finish_year*100+month
            end
        else
            for month = start_month, finish_month do
                result[#result+1] = start_year*100+month
            end
        end
    else 
        local ss
        if slot == 'd' then
            ss = SECONDS_IN_A_DAY
        else
            ss = tonumber(slot)
        end

        start  = slot_to_ts(key_year_ts, tostring(start))
        finish = slot_to_ts(key_year_ts, tostring(finish))
        start = start - start % ss
        finish = finish - finish % ss
        for i = start, finish, slot do
            result[#result+1] = ts_to_slot(key_year_ts, i, slot)
        end
    end

    return result
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
