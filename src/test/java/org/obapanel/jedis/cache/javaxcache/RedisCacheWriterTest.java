package org.obapanel.jedis.cache.javaxcache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.cache.javaxcache.MockOfJedisForRedisCache.unitTestEnabledForRedisCache;

@RunWith(MockitoJUnitRunner.Silent.class)
public class RedisCacheWriterTest {

    private MockOfJedisForRedisCache mockOfJedisForRedisCache;

    private final TestingCacheWriter testingCacheWriter = new TestingCacheWriter();

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
        RedisCache redisCache = redisCacheManager.createRedisCache(name, new RedisCacheConfiguration());
        redisCache.setCacheWriter(testingCacheWriter);
        return redisCache;
    }

    @Test
    public void putTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void putWithErrorTest() {
        RedisCache redisCache = createNewCache();
        testingCacheWriter.doNextError();
        redisCache.put("a","A1");
    }

    @Test
    public void getAndPutTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a", "A1");
        String oldResult = redisCache.getAndPut("a", "A2");
        assertEquals("A1", oldResult);
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void putAllTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        redisCache.putAll(data);
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        assertEquals("B1", redisCache.get("b"));
        assertEquals("B1", testingCacheWriter.get("b"));
        assertEquals("C1", redisCache.get("c"));
        assertEquals("C1", testingCacheWriter.get("c"));
        assertEquals(3, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void putAllWithErrorTest() {
        RedisCache redisCache = createNewCache();
        testingCacheWriter.doNextError();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        redisCache.putAll(data);
    }

    @Test
    public void putIfAbsentTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        redisCache.putAll(data);
        boolean putResult1 = redisCache.putIfAbsent("d","D2");
        boolean putResult2 = redisCache.putIfAbsent("a","A2");
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        assertEquals("B1", redisCache.get("b"));
        assertEquals("B1", testingCacheWriter.get("b"));
        assertEquals("C1", redisCache.get("c"));
        assertEquals("C1", testingCacheWriter.get("c"));
        assertEquals("D2", redisCache.get("d"));
        assertEquals("D2", testingCacheWriter.get("d"));
        assertTrue(putResult1);
        assertFalse(putResult2);
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void removeTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        boolean result = redisCache.remove("a");
        assertTrue(result);
        assertFalse(redisCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(1, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void removeWithErrorTest() {
        RedisCache redisCache = createNewCache();
        testingCacheWriter.doNextError();
        redisCache.put("a","A1");
        redisCache.remove("a");
    }


    @Test
    public void removeWithValueTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        boolean result1 = redisCache.remove("a", "A2");
        assertFalse(result1);
        assertTrue(redisCache.containsKey("a"));
        assertNotNull(testingCacheWriter.get("a"));
        boolean result2 = redisCache.remove("a", "A1");
        assertTrue(result2);
        assertFalse(redisCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(1, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void getAndRemoveTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        String oldResult = redisCache.getAndRemove("a");
        assertEquals("A1", oldResult);
        assertFalse(redisCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(1, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void replaceTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        boolean result = redisCache.replace("a", "A2");
        assertTrue(result);
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void replaceWithValueTest() {
        RedisCache redisCache = createNewCache();
        redisCache.put("a","A1");
        boolean result1 = redisCache.replace("a", "A3", "A2");
        assertFalse(result1);
        assertEquals("A1", redisCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        boolean result2 = redisCache.replace("a", "A1", "A2");
        assertTrue(result2);
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void getAndReplaceTest() {
        RedisCache redisCache = createNewCache();
        String result1 = redisCache.getAndReplace("a","A2");
        assertNull(result1);
        assertNull( redisCache.get("a"));
        assertNull( testingCacheWriter.get("a"));
        redisCache.put("a","A1");
        String result2 = redisCache.getAndReplace("a","A2");
        assertEquals(result2, "A1");
        assertEquals("A2", redisCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void removeAllTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        redisCache.putAll(data);
        redisCache.removeAll(new HashSet<>(Arrays.asList("a","b","c")));
        assertFalse(redisCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertFalse(redisCache.containsKey("b"));
        assertNull(testingCacheWriter.get("b"));
        assertFalse(redisCache.containsKey("c"));
        assertNull(testingCacheWriter.get("c"));
        assertTrue(redisCache.containsKey("d"));
        assertNotNull(testingCacheWriter.get("d"));
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(3, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void removeAllWithErrorTest() {
        RedisCache redisCache = createNewCache();
        testingCacheWriter.doNextError();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        redisCache.putAll(data);
        redisCache.removeAll(new HashSet<>(Arrays.asList("a","b","c")));
    }

    @Test
    public void removeAllAllTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        redisCache.putAll(data);
        redisCache.removeAll();
        assertFalse(redisCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertFalse(redisCache.containsKey("b"));
        assertNull(testingCacheWriter.get("b"));
        assertFalse(redisCache.containsKey("c"));
        assertNull(testingCacheWriter.get("c"));
        assertFalse(redisCache.containsKey("d"));
        assertNull(testingCacheWriter.get("d"));
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(4, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void clearlTest() {
        RedisCache redisCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        redisCache.putAll(data);
        redisCache.clear();
        assertFalse(redisCache.containsKey("a"));
        assertNotNull(testingCacheWriter.get("a"));
        assertFalse(redisCache.containsKey("b"));
        assertNotNull(testingCacheWriter.get("b"));
        assertFalse(redisCache.containsKey("c"));
        assertNotNull(testingCacheWriter.get("c"));
        assertFalse(redisCache.containsKey("d"));
        assertNotNull(testingCacheWriter.get("d"));
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    /*
    removeAll
            removeAll
*/


    private static class TestingCacheWriter implements CacheWriter<String, String> {

        private final Map<String, String> internalData = new HashMap<>();

        private final AtomicBoolean launchErrorInWriter = new AtomicBoolean(false);

        private final AtomicInteger dataInserted = new AtomicInteger(0);
        private final AtomicInteger dataDeleted = new AtomicInteger(0);



        public void doNextError() {
            launchErrorInWriter.set(true);
        }

        public int countDataInserted() {
            return dataInserted.get();
        }

        public int countDataDeleted() {
            return dataDeleted.get();
        }


        public String get(String key) {
            return internalData.get(key);
        }

        public boolean contains(String key) {
            return internalData.containsKey(key);
        }



        @Override
        public void write(Cache.Entry<? extends String, ? extends String> entry) throws CacheWriterException {
            doWaitOrError();
            dataInserted.incrementAndGet();
            internalData.put(entry.getKey(), entry.getValue());
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends String, ? extends String>> entries) throws CacheWriterException {
            entries.forEach( this::write);
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            doWaitOrError();
            dataDeleted.incrementAndGet();
            internalData.remove(key);
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
            keys.forEach(this::delete);
        }

        private synchronized void doWaitOrError() {
            if (launchErrorInWriter.get()) {
                throw new IllegalStateException("Test error");
            }
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(10));
            } catch (InterruptedException e) {
                // Eat this
            }
        }

    }



}
