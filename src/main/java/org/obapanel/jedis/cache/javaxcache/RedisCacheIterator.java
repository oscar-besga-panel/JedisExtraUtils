package org.obapanel.jedis.cache.javaxcache;

import org.obapanel.jedis.iterators.ScanIterator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.cache.Cache;
import java.util.Iterator;

class RedisCacheIterator implements Iterator<Cache.Entry<String, String>> {

    private final RedisCache redisCache;
    private final JedisPool jedisPool;
    private final ScanIterator scanIterator;

    RedisCacheIterator(RedisCache redisCache, JedisPool jedisPool) {
        this.redisCache = redisCache;
        this.jedisPool = jedisPool;
        this.scanIterator = new ScanIterator(jedisPool, redisCache.resolveKey("*"));
    }

    @Override
    public boolean hasNext() {
        return scanIterator.hasNext();
    }

    @Override
    public Cache.Entry<String, String> next() {
        String redisKey = scanIterator.next();
        if (redisKey != null) {
            try(Jedis jedis = jedisPool.getResource()) {
                String value = jedis.get(redisKey);
                String key = redisCache.unresolveKey(redisKey);
                return new RedisCacheEntry(key, value);
            }
        } else {
            return null;
        }
    }

}
