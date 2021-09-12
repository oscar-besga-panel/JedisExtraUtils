package org.obapanel.jedis.cache;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

public class Prueba {

    public void x() {
        RedisCachingProvider redisCachingProvider = new RedisCachingProvider();
        CachingProvider provider = Caching.getCachingProvider(RedisCachingProvider.class.getName());
        CacheManager manager = provider.getCacheManager();
        Configuration<String, String> configuration = new RedisCacheConfiguration();
        RedisCache redisCache = (RedisCache) manager.createCache("redisCache",configuration);


    }

    public static void main(String[] args) {
        new Prueba().x();
    }

}
