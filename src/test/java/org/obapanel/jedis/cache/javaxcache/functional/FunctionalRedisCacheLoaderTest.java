package org.obapanel.jedis.cache.javaxcache.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.obapanel.jedis.cache.javaxcache.RedisCache;
import org.obapanel.jedis.cache.javaxcache.RedisCacheConfiguration;
import org.obapanel.jedis.cache.javaxcache.RedisCacheManager;
import org.obapanel.jedis.cache.javaxcache.RedisCachingProvider;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.Silent.class)
public class FunctionalRedisCacheLoaderTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalRedisCacheLoaderTest.class);


    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private final TestingCacheLoader testingCacheLoader = new TestingCacheLoader();

    private JedisPool jedisPool;



    @Before
    public void before() {
        RedisCachingProvider.getInstance().getRedisCacheManager();
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) {
            jedisPool.close();
            RedisCachingProvider.getInstance().getRedisCacheManager().clearJedisPool();
        }
    }

    RedisCache createNewCache() {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        RedisCachingProvider redisCachingProvider = RedisCachingProvider.getInstance();
        RedisCacheManager redisCacheManager = redisCachingProvider.getRedisCacheManager();
        if (!redisCacheManager.hasJedisPool()) {
            redisCacheManager.setJedisPool(jedisPool);
        }
        RedisCache redisCache = redisCacheManager.createRedisCache(name, new RedisCacheConfiguration());
        redisCache.setCacheLoader(testingCacheLoader);
        return redisCache;
    }

    @Test
    public void getCacheLoaderTest() {
        RedisCache redisCache = createNewCache();
        assertNotNull(redisCache.getCacheLoader());
    }

    @Test
    public void loadAllTest() throws InterruptedException {
        final AtomicBoolean error = new AtomicBoolean(false);
        final Semaphore semaphore = new Semaphore(0);
        RedisCache redisCache = createNewCache();
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        redisCache.loadAll(keys, true, new CompletionListener() {
            @Override
            public void onCompletion() {
                semaphore.release();
            }

            @Override
            public void onException(Exception e) {
                error.set(true);
                semaphore.release();
            }
        });
        boolean end = semaphore.tryAcquire(60, TimeUnit.SECONDS);
        assertTrue(end);
        assertTrue(!error.get());
        assertTrue(redisCache.containsKey("a"));
        assertTrue(redisCache.containsKey("b"));
        assertTrue(redisCache.containsKey("c"));
        assertEquals(3, testingCacheLoader.countDataGenerator());
    }

    @Test
    public void loadAllWithErrorTest() throws InterruptedException {
        final AtomicBoolean error = new AtomicBoolean(false);
        final Semaphore semaphore = new Semaphore(0);
        RedisCache redisCache = createNewCache();
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        testingCacheLoader.doNextError();
        redisCache.loadAll(keys, true, new CompletionListener() {
            @Override
            public void onCompletion() {
                semaphore.release();
            }

            @Override
            public void onException(Exception e) {
                error.set(true);
                semaphore.release();
            }
        });
        boolean end = semaphore.tryAcquire(60, TimeUnit.SECONDS);
        assertTrue(end);
        assertTrue(error.get());
        assertFalse(redisCache.containsKey("a"));
        assertFalse(redisCache.containsKey("b"));
        assertFalse(redisCache.containsKey("c"));
        assertEquals(0, testingCacheLoader.countDataGenerator());
    }

    @Test
    public void loadAllNowTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        redisCache.loadAllNow(keys, true);
        assertTrue(redisCache.containsKey("a"));
        assertNotEquals("A1", redisCache.get("a"));
        assertTrue(redisCache.containsKey("b"));
        assertTrue(redisCache.containsKey("c"));
        assertEquals(3, testingCacheLoader.countDataGenerator());
    }

    @Test(expected = IllegalStateException.class)
    public void loadAllNowWithErrorTest() {
        testingCacheLoader.doNextError();
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        redisCache.loadAllNow(keys, true);
    }

    @Test
    public void loadAllNowWithExistingValuesTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        redisCache.loadAllNow(keys, false);
        assertEquals("A1", redisCache.get("a"));
        assertTrue(redisCache.containsKey("a"));
        assertTrue(redisCache.containsKey("b"));
        assertTrue(redisCache.containsKey("c"));
        assertEquals(2, testingCacheLoader.countDataGenerator());
    }

    @Test
    public void getTest() {
        RedisCache redisCache = createNewCache();
        String result = redisCache.get("a");
        assertNotNull(result);
        assertEquals(1, testingCacheLoader.countDataGenerator());
    }


    @Test
    public void getWithDataTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a", "A1");
        String result = redisCache.get("a");
        assertNotNull(result);
        assertEquals(0, testingCacheLoader.countDataGenerator());
    }

    @Test(expected = IllegalStateException.class)
    public void getWithErrorTest() {
        testingCacheLoader.doNextError();
        RedisCache redisCache = createNewCache();
        redisCache.get("a");
    }


    private static class TestingCacheLoader implements CacheLoader<String, String> {

        private final Map<String, String> internalData = new HashMap<>();

        private final AtomicBoolean launchErrorInLoader = new AtomicBoolean(false);

        private final AtomicInteger dataGenerated = new AtomicInteger(0);



        public void doNextError() {
            launchErrorInLoader.set(true);
        }

        public int countDataGenerator() {
            return dataGenerated.get();
        }

        @Override
        public String load(String key) throws CacheLoaderException {
            doWait();
            return internalData.computeIfAbsent( key, this::computeIfAbsent );
        }

        private String computeIfAbsent(String key) {
            if (launchErrorInLoader.get()) {
                launchErrorInLoader.set(false);
                throw new IllegalStateException("Test exception");
            } else {
                doWait();
                dataGenerated.incrementAndGet();
                return key + ":" + System.currentTimeMillis();
            }
        }

        @Override
        public Map<String, String> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            Map<String, String> results = new HashMap<>();
            keys.forEach( key -> results.put(key, load(key)));
            return results;
        }

        private synchronized void doWait() {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(50));
            } catch (InterruptedException e) {
                // Eat this
            }
        }
    }

}
