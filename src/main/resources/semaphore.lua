-- for class org.oba.jedis.extra.utils.semaphore.Semaphore
local permits = redis.call('get', KEYS[1])
if (permits ~= false and tonumber(permits) >= tonumber(ARGV[1])) then
    redis.call('decrby', KEYS[1], ARGV[1])
    return 'true'
else
    return 'false'
end