-- for class org.oba.jedis.extra.utils.rateLimiter.ThrottlingRateLimiter

  -- get the throttlingRateLimiter name
  local name = KEYS[1]
redis.log(redis.LOG_WARNING, 'throttlingRateLimiter.lua > name ' .. name)
redis.call('ECHO', 'throttlingRateLimiter.lua > name ' .. name)
  -- get the local timestamp
  local tst = redis.call('time')
  local ts = (tst[1] * 1000000) + tst[2]
  local allow = false
  -- get last time allowed to pass call
  local last_allow_micros = redis.call('hget', name, 'last_allow_micros')
  local allow_micros = redis.call('hget', name, 'allow_micros')
redis.log(redis.LOG_WARNING, 'ts ' .. ts .. ' last_allow_micros ' .. last_allow_micros .. ' allow_micros ' .. allow_micros)
redis.call('ECHO',  'ts ' .. ts .. ' last_allow_micros ' .. last_allow_micros .. ' allow_micros ' .. allow_micros)
  if (last_allow_micros + allow_micros < ts) then
      -- allow if more time has passed and update throttlingRateLimiter data
      allow = true
      redis.call('hset', name, 'last_allow_micros', ts)
redis.log(redis.LOG_WARNING, 'allowed, last_allow_micros is ts ' .. ts)
redis.call('ECHO',  'allowed, last_allow_micros is ts ' .. ts)
  end
  -- end script
redis.log(redis.LOG_WARNING, 'throttlingRateLimiter.lua < allow ' .. tostring(allow))
redis.call('ECHO', 'throttlingRateLimiter.lua < allow ' .. tostring(allow))
  return allow