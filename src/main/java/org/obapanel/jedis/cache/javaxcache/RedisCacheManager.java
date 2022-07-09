package org.obapanel.jedis.cache.javaxcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RedisCacheManager implements CacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCacheManager.class);


    private static class HOLDER {
        private static final RedisCacheManager INSTANCE;
        static {
            INSTANCE = (RedisCacheManager) RedisCachingProvider.getInstance().getCacheManager();
        }
    }

    public static RedisCacheManager getInstance() {
        return HOLDER.INSTANCE;
    }

    private final RedisCachingProvider redisCachingProvider;
    private final URI uri;
    private final ClassLoader classLoader;
    private final Properties properties;
    private final Map<String, RedisCache> redisCacheMap = new HashMap<>();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private JedisPool jedisPool;

    RedisCacheManager(RedisCachingProvider redisCachingProvider, URI uri, ClassLoader classloader,
                       Properties properties) {
        this.redisCachingProvider = redisCachingProvider;
        this.uri = uri;
        this.classLoader = classloader;
        this.properties = properties;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void setJedisPool(JedisPool jedisPool) {
        if (this.jedisPool != null) LOGGER.warn("JedisPool was not null, replacing");
        this.jedisPool = jedisPool;
    }

    public void clearJedisPool() {
        jedisPool = null;
    }

    public boolean hasJedisPool() {
        return jedisPool != null;
    }


    @Override
    public CachingProvider getCachingProvider() {
        return redisCachingProvider;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    public RedisCache createRedisCache(String cacheName, RedisCacheConfiguration configuration) throws IllegalArgumentException {
        return (RedisCache) createCache(cacheName, configuration);
    }


    @SuppressWarnings("ALL")
    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        checkClosed();
        if (cacheName == null) throw new NullPointerException("Cachename must not be null");
        if (configuration == null) throw new NullPointerException("Configuration must not be null");
        return (Cache) redisCacheMap.computeIfAbsent(cacheName, name ->
                new RedisCache(name, (RedisCacheConfiguration) configuration, this)
        );
    }

    public RedisCache getRedisCache(String cacheName) {
        checkClosed();
        if (cacheName == null) throw new NullPointerException("Cachename must not be null");
        return redisCacheMap.get(cacheName);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        if (!keyType.equals(String.class)) throw new UnsupportedOperationException("Only RedisCache <- Cache<String,String> available");
        if (!valueType.equals(String.class)) throw new UnsupportedOperationException("Only RedisCache <- Cache<String,String> available");
        return (Cache<K, V>) getRedisCache(cacheName);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        return (Cache<K, V>) getRedisCache(cacheName);
    }

    @Override
    public Iterable<String> getCacheNames() {
        checkClosed();
        return new ArrayList<>(redisCacheMap.keySet());
    }

    @Override
    public void destroyCache(String cacheName) {
        checkClosed();
        RedisCache redisCache = redisCacheMap.get(cacheName);
        if (redisCache != null) {
            redisCache.close();
            redisCacheMap.remove(cacheName);
        }
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        checkClosed();
        throw new UnsupportedOperationException("Operation not available");
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        checkClosed();
        throw new UnsupportedOperationException("Operation not available");
    }

    @Override
    public void close() {
        isClosed.set(true);
        redisCacheMap.values().forEach( c -> c.close());
        redisCacheMap.clear();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * Checks if manager is closed to allow opertation
     * @throws IllegalStateException if manager is closed
     */
    private void checkClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("RedisCachingManager is closed");
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return RedisCacheUtils.unwrap(clazz, this);
    }

}
