-- for class org.oba.jedis.extra.utils.cycle.CycleData

local name = KEYS[1]
local current = redis.call('hget', name, 'current')
local size = redis.call('hlen', name) - 1
local result = redis.call('hget', name, tostring(current))
current = current + 1
if (current >= size) then
    current = 0
end
redis.call('hset', name, 'current', current)
return result