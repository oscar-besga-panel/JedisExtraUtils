package org.obapanel.jedis.cache.javaxcache;

import org.obapanel.jedis.iterators.ScanIterator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.cache.Cache;
import java.util.Iterator;

class RedisCacheIterator implements Iterator<Cache.Entry<String, String>> {
    private final JedisPool jedisPool;
    private final ScanIterator scanIterator;

    RedisCacheIterator(RedisCache redisCache, JedisPool jedisPool){
        this.jedisPool = jedisPool;
        this.scanIterator = new ScanIterator(jedisPool, redisCache.resolveKey("*"));
    }

    @Override
    public boolean hasNext() {
        return scanIterator.hasNext();
    }

    @Override
    public Cache.Entry<String, String> next() {
        String key = scanIterator.next();
        if (key != null) {
            try(Jedis jedis = jedisPool.getResource()) {
                String value = jedis.get(key);
                return new SimpleEntry(key, value);
            }
        } else {
            return null;
        }
    }

}
