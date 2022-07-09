package org.obapanel.jedis.cache.javaxcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.obapanel.jedis.utils.UnmodifiableProperties.EMPTY_UNMODIFIABLE_PROPERTIES;

/**
 * A class that implements a simple provider for redis cache objects
 * Made as simple as possible
 * It has a default instance with the most default properties
 * Use getInstance and don't get lost
 */
public final class RedisCachingProvider implements CachingProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCachingProvider.class);

    public static final ClassLoader DEFAULT_CLASSLOADER;
    public static final URI DEFAULT_URI;
    public static final Properties DEFAULT_PROPERTIES = EMPTY_UNMODIFIABLE_PROPERTIES;

    static {
        try {
            DEFAULT_CLASSLOADER = RedisCachingProvider.class.getClassLoader();
            DEFAULT_URI = new URI(RedisCachingProvider.class.getName());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Bad URI for cache",e);
        }
    }

    private static class HOLDER {
        private static final RedisCachingProvider INSTANCE;
        static {
            INSTANCE = new RedisCachingProvider();
            INSTANCE.getCacheManager();
        }
    }

    public static RedisCachingProvider getInstance() {
        return HOLDER.INSTANCE;
    }


    private final ClassLoader defaultClassloader;
    private final URI defaultUri;
    private final Properties defaultProperties;
    private final WeakHashMap<ClassLoader, Map<URI,CacheManager>> cacheManagers = new WeakHashMap<>();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);


    /**
     * Basic, inside constructor
     */
    private RedisCachingProvider() {
        this(DEFAULT_CLASSLOADER, DEFAULT_URI, DEFAULT_PROPERTIES);
    }

    /**
     * Provider for default URI and properties
     * @param defaultUri uri
     * @param defaultProperties properties
     */
    public RedisCachingProvider(URI defaultUri, Properties defaultProperties) {
        this(DEFAULT_CLASSLOADER, defaultUri, defaultProperties);
    }

    /**
     * Provider for default URI and properties and classloader
     * @param defaultClassloader classlaoder
     * @param defaultUri uri
     * @param defaultProperties properties
     */
    public RedisCachingProvider(ClassLoader defaultClassloader, URI defaultUri, Properties defaultProperties) {
        this.defaultClassloader = defaultClassloader;
        this.defaultUri = defaultUri;
        this.defaultProperties = defaultProperties;
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        checkClosed();
        Map<URI, CacheManager> uriMap = cacheManagers.computeIfAbsent(classLoader, c -> new HashMap<>());
        return uriMap.computeIfAbsent(uri, u -> new RedisCacheManager(this, uri, classLoader, properties));
    }


    @Override
    public ClassLoader getDefaultClassLoader() {
        return defaultClassloader;
    }

    @Override
    public URI getDefaultURI() {
        return defaultUri;
    }

    @Override
    public Properties getDefaultProperties() {
        return defaultProperties;
    }


    /**
     * Request a cache manager for a URI
     * @param uri Given URI
     * @return Exisiting or new cache manager
     */
    public CacheManager getCacheManager(URI uri) {
        checkClosed();
        return getCacheManager(uri, defaultClassloader, defaultProperties);
    }

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        checkClosed();
        return getCacheManager(uri, classLoader, defaultProperties);
    }

    @Override
    public CacheManager getCacheManager() {
        checkClosed();
        return getCacheManager(defaultUri, defaultClassloader, defaultProperties);
    }

    public RedisCacheManager getRedisCacheManager() {
        return (RedisCacheManager) getCacheManager();
    }

    @Override
    public void close() {
        isClosed.set(true);
        cacheManagers.values().forEach( m ->{
            m.values().forEach( cm -> cm.close());
            m.clear();
        });
        cacheManagers.clear();
    }

    @Override
    public void close(ClassLoader classLoader) {
        close(defaultUri, classLoader);
    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
        getCacheManager(uri, classLoader).close();
        cacheManagers.get(classLoader).remove(uri);
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }

    /**
     * Checks if provider is closed to allow opertation
     * @throws IllegalStateException if provider is closed
     */
    private void checkClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("RedisCachingProvider is closed");
        }
    }

}
