package org.obapanel.jedis.cache.javaxcache;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.time.Duration;

public class Testing_Main_Class_ToBeDeleted {


    public JedisPool createJedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(24); // original 128
        jedisPoolConfig.setMaxIdle(24); // original 128
        jedisPoolConfig.setMinIdle(4); // original 16
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        jedisPoolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        jedisPoolConfig.setNumTestsPerEvictionRun(3);
        jedisPoolConfig.setBlockWhenExhausted(true);
        return new JedisPool(jedisPoolConfig, "127.0.0.1", 6379);
    }

    public void doSsomething() {
        RedisCachingProvider redisCachingProvider = new RedisCachingProvider(createJedisPool());
        CachingProvider provider = Caching.getCachingProvider(RedisCachingProvider.class.getName());
        CacheManager manager = provider.getCacheManager();
        Configuration<String, String> configuration = new RedisCacheConfiguration();
        RedisCache redisCache = (RedisCache) manager.createCache("redisCache",configuration);


    }

    public static void main(String[] args) {
        new Testing_Main_Class_ToBeDeleted().doSsomething();
    }

}
