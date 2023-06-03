-- for class org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter

-- get bucketRateLimiter name and permits to get
local name = KEYS[1]
local permits = tonumber(ARGV[1])
redis.log(redis.LOG_WARNING, 'bucketRateLimiter.lua > name ' .. name .. ' permits ' .. permits)
redis.call('ECHO', 'bucketRateLimiter.lua > name ' .. name .. ' permits ' .. permits)
-- get local timestamp
local tst = redis.call('time')
local ts = (tst[1] * 1000000) + tst[2]
local num_refill = 0
local capacity = tonumber(redis.call('hget', name, 'capacity'))
redis.log(redis.LOG_WARNING, 'capacity ' .. capacity)
redis.call('ECHO', 'capacity ' .. capacity)
local mode = redis.call('hget', name, 'mode')
redis.log(redis.LOG_WARNING, 'ts ' .. ts .. ' mode ' .. mode)
redis.call('ECHO', 'ts ' .. ts .. ' mode ' .. mode)
local last_refill_micros = redis.call('hget', name, 'last_refill_micros')
local refill_micros = redis.call('hget', name, 'refill_micros')
redis.log(redis.LOG_WARNING, 'ts ' .. ts .. ' last_refill_micros ' .. last_refill_micros .. ' refill_micros ' .. refill_micros)
redis.call('ECHO',  'ts ' .. ts .. ' last_refill_micros ' .. last_refill_micros .. ' refill_micros ' .. refill_micros)
if string.upper(mode) == 'GREEDY' then
    -- execute greedy refill
    redis.log(redis.LOG_WARNING, 'refill greedy ')
    redis.call('ECHO', 'refill greedy')
    local refill_percentaje = (ts - last_refill_micros) / refill_micros
    if (refill_percentaje > 1) then
        refill_percentaje = 1
    end
    num_refill = capacity * refill_percentaje
    redis.log(redis.LOG_WARNING, 'refill greedy num_refill ' .. num_refill .. ' refill_percentaje ' .. refill_percentaje)
    redis.call('ECHO', 'refill greedy num_refill ' .. num_refill .. ' refill_percentaje ' .. refill_percentaje)
elseif string.upper(mode) == 'INTERVAL' then
    -- execute interval refill
    redis.log(redis.LOG_WARNING, 'refill internal ')
    redis.call('ECHO', 'refill interval')
    if (last_refill_micros + refill_micros < ts) then
        -- allow interval refill
        num_refill = capacity
    end
    redis.log(redis.LOG_WARNING, 'refill interval numRefill ' .. num_refill)
    redis.call('ECHO', 'refill interval numRefill ' .. num_refill)
end
if (num_refill > 0) then
    -- execute refill and update available
    redis.log(redis.LOG_WARNING, 'refill ok ')
    redis.call('ECHO', 'refill ok')
    local newAvailable = num_refill + tonumber(redis.call('hget', name, 'available'))
    redis.log(redis.LOG_WARNING, 'newAvailable ' .. newAvailable)
    redis.call('ECHO', 'newAvailable ' .. newAvailable)
    redis.log(redis.LOG_WARNING, 'capacity ' .. capacity)
    redis.call('ECHO', 'capacity ' .. capacity)
    if (newAvailable > capacity) then
        newAvailable = capacity
    end
    redis.call('hset', name, 'available', newAvailable)
    redis.log(redis.LOG_WARNING, 'newAvailable set ' .. newAvailable)
    redis.call('ECHO', 'newAvailable set ' .. newAvailable)
    redis.call('hset', name, 'last_refill_micros', ts)
    redis.log(redis.LOG_WARNING, 'last_refill_micros ' .. ts)
    redis.call('ECHO', 'last_refill_micros ' .. ts)
end
-- try acquire
local acquire = false
local available = tonumber(redis.call('hget', name, 'available'))
redis.log(redis.LOG_WARNING, 'available ' .. available .. ' permits ' .. permits)
redis.call('ECHO',  'available ' .. available .. ' permits ' .. permits)
if (available >= permits) then
    -- allow to acquiere and upadte new available
    available = available - permits
    redis.call('hset', name, 'available', available)
    acquire = true
    redis.log(redis.LOG_WARNING, 'bucketRateLimiter.lua < acquiredOk available ' .. available .. ' acquire ' .. tostring(acquire))
    redis.call('ECHO',  'bucketRateLimiter.lua < acquiredOk available ' .. available .. ' acquire ' .. tostring(acquire))
else
    redis.log(redis.LOG_WARNING, 'bucketRateLimiter.lua < acquiredNO available ' .. available .. ' acquire ' .. tostring(acquire))
    redis.call('ECHO',  'bucketRateLimiter.lua < acquiredNO available ' .. available .. ' acquire ' .. tostring(acquire))
end
-- end script
return acquire