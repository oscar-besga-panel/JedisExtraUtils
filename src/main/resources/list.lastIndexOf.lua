-- org.oba.jedis.extra.utils.collections.JedisList lastIndexOf
local key = KEYS[1]
local obj = ARGV[1]
local poss = -1
local items = redis.call('lrange', key, 0, -1)
for i=1,#items do
    if items[i] == obj then
        poss = i - 1
    end
end
return poss
