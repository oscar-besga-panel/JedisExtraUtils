-- for class org.oba.jedis.extra.utils.utils.ScriptHolderTest
local tst = redis.call('time')
redis.log(redis.LOG_WARNING, 'scriptHolderTest.lua > tst ' .. tst[1])
redis.call('ECHO', 'scriptHolderTest.lua > tst ' .. tst[1])
return tst
