package org.obapanel.jedis.cache;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class RedisCachingProvider implements CachingProvider {

    public static final String NAME_URL ="org.obapanel.jedis.cache";
    private static final URI BASE_URI;

    static {
        try {
            BASE_URI = new URI("cache://" + NAME_URL);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Bad URI for cache",e);
        }
    }

    String defaultUriName = NAME_URL;
    URI defaultUri = BASE_URI;
    Properties defaultProperties;
    RedisCacheManager defaultRedisCacheManager;

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        return defaultRedisCacheManager;
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return this.getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        return defaultUri;
    }

    @Override
    public Properties getDefaultProperties() {
        return defaultProperties;
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return defaultRedisCacheManager;
    }

    @Override
    public CacheManager getCacheManager() {
        return defaultRedisCacheManager;
    }

    @Override
    public void close() {
        //NOPE
    }

    @Override
    public void close(ClassLoader classLoader) {
        close();
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        close();
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }
}
