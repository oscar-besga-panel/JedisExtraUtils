-- for class org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter

-- get bucketRateLimiter name and permits to get
local name = KEYS[1]
local permits = tonumber(ARGV[1])
-- get local timestamp
local tst = redis.call('time')
local ts = (tst[1] * 1000000) + tst[2]
local num_refill = 0
local capacity = tonumber(redis.call('hget', name, 'capacity'))
local mode = redis.call('hget', name, 'mode')
local last_refill_micros = redis.call('hget', name, 'last_refill_micros')
local refill_micros = redis.call('hget', name, 'refill_micros')
if string.upper(mode) == 'GREEDY' then
    -- execute greedy refill
    local refill_percentaje = (ts - last_refill_micros) / refill_micros
    if (refill_percentaje > 1) then
        refill_percentaje = 1
    end
    num_refill = capacity * refill_percentaje
elseif string.upper(mode) == 'INTERVAL' then
    -- execute interval refill
    if (last_refill_micros + refill_micros < ts) then
        -- allow interval refill
        num_refill = capacity
    end
end
if (num_refill > 0) then
    -- execute refill and update available
    local newAvailable = num_refill + tonumber(redis.call('hget', name, 'available'))
    if (newAvailable > capacity) then
        newAvailable = capacity
    end
    redis.call('hset', name, 'available', newAvailable)
    redis.call('hset', name, 'last_refill_micros', ts)
end
-- try acquire
local acquire = false
local available = tonumber(redis.call('hget', name, 'available'))
if (available >= permits) then
    -- allow to acquiere and upadte new available
    available = available - permits
    redis.call('hset', name, 'available', available)
    acquire = true
end
-- end script
return acquire