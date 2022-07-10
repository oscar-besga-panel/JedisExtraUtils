package org.obapanel.jedis.cache.javaxcache;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import javax.cache.Cache;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.cache.javaxcache.MockOfJedisForRedisCache.unitTestEnabledForRedisCache;

@RunWith(MockitoJUnitRunner.Silent.class)
public class RedisCacheIteratorTest {


    private MockOfJedisForRedisCache mockOfJedisForRedisCache;

    @Before
    public void setup() {
        RedisCachingProvider.getInstance().getRedisCacheManager();
        org.junit.Assume.assumeTrue(unitTestEnabledForRedisCache());
        if (!unitTestEnabledForRedisCache()) return;
        mockOfJedisForRedisCache = new MockOfJedisForRedisCache();
    }

    @After
    public void tearDown() {
        if (mockOfJedisForRedisCache != null) {
            mockOfJedisForRedisCache.clearData();
            RedisCachingProvider.getInstance().getRedisCacheManager().clearJedisPool();
        }
    }

    RedisCache createNewCache() {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        RedisCachingProvider redisCachingProvider = RedisCachingProvider.getInstance();
        RedisCacheManager redisCacheManager = redisCachingProvider.getRedisCacheManager();
        if (!redisCacheManager.hasJedisPool()) {
            redisCacheManager.setJedisPool(mockOfJedisForRedisCache.getJedisPool());
        }
        return redisCacheManager.createRedisCache(name, new RedisCacheConfiguration());
    }

    @Test
    public void getIteratorTest() {
        RedisCache redisCache = createNewCache();
        assertNotNull(redisCache.iterator());
    }

    @Test
    public void iteratorTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        redisCache.putAll(data);
        int count = 0;
        Iterator<Cache.Entry<String, String>> it = redisCache.iterator();
        while (it.hasNext()) {
            Cache.Entry<String, String> entry = it.next();
            assertTrue(data.keySet().contains(entry.getKey()));
            assertTrue(data.values().contains(entry.getValue()));
            count++;
        }
        assertEquals(3, count);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeTest() {
        RedisCache redisCache = createNewCache();
        redisCache.iterator().remove();
    }

    @Test
    public void forEachRemainingTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        redisCache.putAll(data);
        AtomicInteger count = new AtomicInteger(0);
        redisCache.iterator().forEachRemaining( entry -> {
            assertTrue(data.keySet().contains(entry.getKey()));
            assertTrue(data.values().contains(entry.getValue()));
            count.incrementAndGet();
        });
        assertEquals(3, count.get());
    }

}
