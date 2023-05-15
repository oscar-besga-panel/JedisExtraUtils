-- for class org.oba.jedis.extra.utils.rateLimiter.BucketRateLimiter


  local name = KEYS[1]
  local permits = tonumber(ARGV[1])
redis.log(redis.LOG_WARNING, 'bucketRateLimiter.lua > name ' .. name .. ' permits ' .. permits)
redis.call('ECHO', 'bucketRateLimiter.lua > name ' .. name .. ' permits ' .. permits)
  -- refill
  local tst = redis.call('time')
  local ts = tst[1] * 1000000 + tst[2]
  local refill = false
  local numRefill = 0
  local mode = redis.call('hget', name, 'mode')
redis.log(redis.LOG_WARNING, 'ts ' .. ts .. ' mode ' .. mode)
redis.call('ECHO', 'ts ' .. ts .. ' mode ' .. mode)
  if refill then
redis.log(redis.LOG_WARNING, 'refill ok ')
redis.call('ECHO', 'refill ok')
    local newAvailable = numRefill + tonumber(redis.call('hget', name, 'available'))
    redis.call('hset', name, 'available', newAvailable)
redis.log(redis.LOG_WARNING, 'newAvailable ' .. newAvailable)
redis.call('ECHO', 'newAvailable .. ' + newAvailable)
  end
  -- try acquire
  local acquire = false
  local available = tonumber(redis.call('hget', name, 'available'))
redis.log(redis.LOG_WARNING, 'available ' .. available .. ' permits ' .. permits)
redis.call('ECHO',  'available ' .. available .. ' permits ' .. permits)
  if (available >= permits) then
    available = available - permits
    redis.call('hset', name, 'available', available)
    acquire = true
    redis.log(redis.LOG_WARNING, 'bucketRateLimiter.lua < acquiredOk available ' .. available .. ' acquire ' .. tostring(acquire))
    redis.call('ECHO',  'bucketRateLimiter.lua < acquiredOk available ' .. available .. ' acquire ' .. tostring(acquire))
  else
    redis.log(redis.LOG_WARNING, 'bucketRateLimiter.lua < acquiredNO available ' .. available .. ' acquire ' .. tostring(acquire))
    redis.call('ECHO',  'bucketRateLimiter.lua < acquiredNO available ' .. available .. ' acquire ' .. tostring(acquire))
  end
  return acquire