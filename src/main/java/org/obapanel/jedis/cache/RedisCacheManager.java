package org.obapanel.jedis.cache;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

public class RedisCacheManager implements CacheManager {

    RedisCachingProvider redisCachingProvider;

    RedisCacheManager(RedisCachingProvider redisCachingProvider) {
        this.redisCachingProvider = redisCachingProvider;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return redisCachingProvider;
    }

    @Override
    public URI getURI() {
        return redisCachingProvider.getDefaultURI();
    }

    @Override
    public ClassLoader getClassLoader() {
        return redisCachingProvider.getDefaultClassLoader();
    }

    @Override
    public Properties getProperties() {
        return redisCachingProvider.getDefaultProperties();
    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        return null;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        return null;
    }

    @Override
    public Iterable<String> getCacheNames() {
        return null;
    }

    @Override
    public void destroyCache(String cacheName) {

    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {

    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }
}
