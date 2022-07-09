package org.obapanel.jedis.cache.javaxcache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisCacheManagerTest {


    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    private RedisCacheConfiguration baseConfiguration = new RedisCacheConfiguration();

    @Before
    public void before() {
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.scan(anyString(), any(ScanParams.class))).
                thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, new ArrayList<>(0)));
    }

    @After
    public void after() {

    }

    private RedisCacheManager generate() {
        URI uri = URI.create("cacheManager:" + this.getClass().getName() + ":" + System.currentTimeMillis());
        RedisCacheManager redisCacheManager = (RedisCacheManager) RedisCachingProvider.getInstance().getCacheManager(uri);
        redisCacheManager.setJedisPool(jedisPool);
        return redisCacheManager;
    }

    @Test
    public void defaultTest() {
        assertNotNull(RedisCacheManager.getInstance());
        assertNotNull(RedisCacheManager.getInstance().getURI());
        assertNotNull(RedisCacheManager.getInstance().getClassLoader());
        assertNotNull(RedisCacheManager.getInstance().getProperties());
        assertEquals(RedisCachingProvider.getInstance().getDefaultURI(), RedisCacheManager.getInstance().getURI());
        assertEquals(RedisCachingProvider.getInstance().getDefaultClassLoader(), RedisCacheManager.getInstance().getClassLoader());
        assertEquals(RedisCachingProvider.getInstance().getDefaultProperties(), RedisCacheManager.getInstance().getProperties());
        assertEquals(RedisCachingProvider.getInstance(), RedisCacheManager.getInstance().getCachingProvider());
    }

    @Test
    public void getAndCreateTest() {
        RedisCacheManager redisCacheManager = generate();
        RedisCache redisCache1 = (RedisCache) redisCacheManager.<String,String>getCache("cache");
        assertNull(redisCache1);
        RedisCache redisCache2 = (RedisCache) redisCacheManager.createCache("cache", baseConfiguration);
        assertNotNull(redisCache2);
        RedisCache redisCache3 = (RedisCache) redisCacheManager.<String,String>getCache("cache");
        assertNotNull(redisCache3);
        RedisCache redisCache4 = redisCacheManager.getRedisCache("cache");
        assertNotNull(redisCache4);
        RedisCache redisCache5 = redisCacheManager.getRedisCache("cache2");
        assertNull(redisCache5);
    }

    @Test
    public void getTest() {
        RedisCacheManager redisCacheManager = generate();
        redisCacheManager.createRedisCache("cache3", baseConfiguration);
        RedisCache redisCache3 = (RedisCache) redisCacheManager.getCache("cache3", String.class, String.class);
        assertNotNull(redisCache3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getTestWithErrors() {
        RedisCacheManager redisCacheManager = generate();
        Cache cache7 = redisCacheManager.getCache("cache7", String.class, Long.class);
    }

    @Test
    public void destroyTest() {
        RedisCacheManager redisCacheManager = generate();
        RedisCache redisCache = (RedisCache) redisCacheManager.createCache("cache", baseConfiguration);
        assertNotNull(redisCache);
        redisCache = (RedisCache) redisCacheManager.<String,String>getCache("cache");
        assertNotNull(redisCache);
        redisCacheManager.destroyCache("cache");
        redisCache = (RedisCache) redisCacheManager.<String,String>getCache("cache");
        assertNull(redisCache);
        try {
            redisCacheManager.destroyCache("cache");
        } catch (Exception e) {
            assertEquals(IllegalStateException.class, e.getClass());
        }
    }

    @Test
    public void getCacheNames() {
        RedisCacheManager redisCacheManager = generate();
        RedisCache redisCache1 = (RedisCache) redisCacheManager.createCache("cache1", baseConfiguration);
        RedisCache redisCache2 = (RedisCache) redisCacheManager.createCache("cache2", baseConfiguration);
        RedisCache redisCache3 = (RedisCache) redisCacheManager.createCache("cache3", baseConfiguration);
        List<String> names = new ArrayList<>();
        redisCacheManager.getCacheNames().forEach( names::add);
        assertTrue(names.contains("cache1"));
        assertTrue(names.contains("cache2"));
        assertTrue(names.contains("cache3"));
        assertNotNull(redisCache1);
        assertNotNull(redisCache2);
        assertNotNull(redisCache3);

    }

    @Test
    public void closeTest() {
        RedisCacheManager redisCacheManager = generate();
        assertNull(redisCacheManager.getCache("cache"));
        redisCacheManager.createCache("cache", baseConfiguration);
        assertNotNull(redisCacheManager.getCache("cache"));
        redisCacheManager.close();
        assertTrue(redisCacheManager.isClosed());
    }

    @Test(expected = IllegalStateException.class)
    public void closeTestWithErrors() {
        RedisCacheManager redisCacheManager = generate();
        assertNull(redisCacheManager.getCache("cache"));
        redisCacheManager.createCache("cache", baseConfiguration);
        assertNotNull(redisCacheManager.getCache("cache"));
        redisCacheManager.close();
        redisCacheManager.getCache("cache");
        fail("Here closed check must have launched an exception");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void enableManagementTest() {
        RedisCacheManager redisCacheManager = generate();
        redisCacheManager.enableManagement("cache", true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void enableStatisticsTest() {
        RedisCacheManager redisCacheManager = generate();
        redisCacheManager.enableStatistics("cache", true);
    }

    @Test
    public void unwarpTest() {
        RedisCacheManager redisCacheManager = generate();
        CacheManager manager = redisCacheManager.unwrap(CacheManager.class);
        assertEquals(RedisCacheManager.class, manager.getClass());
    }


}
