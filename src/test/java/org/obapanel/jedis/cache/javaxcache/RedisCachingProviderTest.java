package org.obapanel.jedis.cache.javaxcache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import javax.cache.configuration.OptionalFeature;
import java.net.URI;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class RedisCachingProviderTest {

    @Before
    public void before() {

    }

    @After
    public void after() {

    }

    @Test
    public void defaultTest() {
        RedisCachingProvider redisCachingProvider = RedisCachingProvider.getInstance();
        assertNotNull(redisCachingProvider);
        assertNotNull(redisCachingProvider.getDefaultClassLoader());
        assertNotNull(redisCachingProvider.getDefaultProperties());
        assertNotNull(redisCachingProvider.getDefaultURI());
    }

    @Test
    public void getTest() {
        RedisCachingProvider redisCachingProvider = RedisCachingProvider.getInstance();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        URI uri = URI.create("x");
        assertNotNull(redisCachingProvider.getCacheManager(uri, classLoader, new Properties()));
        assertNotNull(redisCachingProvider.getCacheManager(uri, classLoader));
        assertNotNull(redisCachingProvider.getCacheManager());
        assertNotNull(RedisCachingProvider.getInstance());
        assertNotNull(RedisCacheManager.getInstance());
        assertTrue(redisCachingProvider.getCacheManager() == RedisCacheManager.getInstance()); // It mus be the same object !
    }

    @Test(expected = IllegalStateException.class)
    public void closeTestWithErrors() {
        RedisCachingProvider redisCachingProvider = new RedisCachingProvider(URI.create("z"), new Properties());
        redisCachingProvider.getCacheManager();
        redisCachingProvider.close();
        redisCachingProvider.getCacheManager();
        fail("Here closed check must have launched an exception");
    }

    @Test
    public void closeTest() {
        RedisCachingProvider redisCachingProvider = RedisCachingProvider.getInstance();
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        URI uri = URI.create("x");
        assertNotNull(redisCachingProvider.getCacheManager(uri, classLoader, new Properties()));
        redisCachingProvider.close(uri, classLoader);
        //TODO more testing
    }

    @Test
    public void isSupportedTest() {
        RedisCachingProvider redisCachingProvider = RedisCachingProvider.getInstance();
        assertFalse(redisCachingProvider.isSupported(OptionalFeature.STORE_BY_REFERENCE));
    }


}
