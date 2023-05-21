-- for class org.oba.jedis.extra.utils.rateLimiter.ThrottlingRateLimiter
local name = KEYS[1]
local tst = redis.call('time')
local ts = (tst[1] * 1000000) + tst[2]
local allow = false
local last_allow_micros = redis.call('hget', name, 'last_allow_micros')
local allow_micros = redis.call('hget', name, 'allow_micros')
if (last_allow_micros + allow_micros < ts) then
    allow = true
    redis.call('hset', name, 'last_allow_micros', ts)
end
return allow