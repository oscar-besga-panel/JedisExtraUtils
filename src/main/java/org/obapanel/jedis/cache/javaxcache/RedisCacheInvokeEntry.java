package org.obapanel.jedis.cache.javaxcache;

import javax.cache.processor.MutableEntry;

class RedisCacheInvokeEntry implements MutableEntry<String, String> {

    private final RedisCache redisCache;
    private final String key;

    RedisCacheInvokeEntry(RedisCache redisCache, String key) {
        this.redisCache = redisCache;
        this.key = key;
    }

    @Override
    public boolean exists() {
        return redisCache.containsKey(key);
    }

    @Override
    public void remove() {
        redisCache.remove(key);
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return redisCache.get(key);
    }

    @Override
    public void setValue(String value) {
        redisCache.put(key, value);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return RedisCacheUtils.unwrap(clazz, this);
    }

}
