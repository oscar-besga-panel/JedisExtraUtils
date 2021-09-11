package org.obapanel.jedis.cache;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

public class Prueba {

    public void x() {
        CachingProvider provider = Caching.getCachingProvider();
        CacheManager manager = provider.getCacheManager();
        Configuration<String, String> configuration = new RedisCacheConfiguration();
        RedisCache redisCache = (RedisCache) manager.createCache("redisCache",configuration);


    }
}
