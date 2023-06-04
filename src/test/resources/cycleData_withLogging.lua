-- for class org.oba.jedis.extra.utils.cycle.CycleData

local name = KEYS[1]
redis.log(redis.LOG_WARNING, 'cycleData.lua > name ' .. name )
redis.call('ECHO', 'cycleData.lua > name ' .. name )
local current = redis.call('hget', name, 'current')
local size = redis.call('hlen', name) - 1
redis.log(redis.LOG_WARNING, 'cycleData.lua > current ' .. current .. ' size ' .. size )
redis.call('ECHO', 'cycleData.lua > current ' .. current .. ' size ' .. size )
local result = redis.call('hget', name, tostring(current))
redis.log(redis.LOG_WARNING, 'cycleData.lua > result ' .. result)
redis.call('ECHO', 'cycleData.lua > result ' .. result)
current = current + 1
if (current >= size) then
    current = 0
end
redis.log(redis.LOG_WARNING, 'cycleData.lua > current next ' .. current)
redis.call('ECHO', 'cycleData.lua > current next ' .. current)
redis.call('hset', name, 'current', current)
return result