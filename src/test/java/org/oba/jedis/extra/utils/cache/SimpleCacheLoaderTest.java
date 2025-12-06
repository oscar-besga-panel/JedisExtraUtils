package org.oba.jedis.extra.utils.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.cache.MockOfJedisForSimpleCache.unitTestEnabledForSimpleCache;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class })
public class SimpleCacheLoaderTest {

    private MockOfJedisForSimpleCache mockOfJedisForsimpleCache;


    private final TestingCacheLoader testingCacheLoader = new TestingCacheLoader();


    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForSimpleCache());
        if (!unitTestEnabledForSimpleCache()) return;
        mockOfJedisForsimpleCache = new MockOfJedisForSimpleCache();
    }

    @After
    public void tearDown() {
        if (mockOfJedisForsimpleCache != null) {
            mockOfJedisForsimpleCache.clearData();
        }
    }

    SimpleCache createNewCache() {
        return createNewCache(testingCacheLoader);
    }
    SimpleCache createNewCache(CacheLoader cacheLoader) {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        return new SimpleCache(mockOfJedisForsimpleCache.getJedisPooled(), name, 3_600_000).
            withCacheLoader(cacheLoader);
    }

    @Test
    public void loadAllTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        simpleCache.loadAll(keys, true);
        assertTrue(simpleCache.containsKey("a"));
        assertNotEquals("A1", simpleCache.get("a"));
        assertTrue(simpleCache.containsKey("b"));
        assertTrue(simpleCache.containsKey("c"));
        assertEquals(3, testingCacheLoader.countDataGenerator());
    }

    @Test
    public void defaultLoadAllTest() {
        CacheLoader baseCacheLoader = (key) -> testingCacheLoader.load(key);
        SimpleCache simpleCache = createNewCache(baseCacheLoader);
        simpleCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        simpleCache.loadAll(keys, true);
        assertTrue(simpleCache.containsKey("a"));
        assertNotEquals("A1", simpleCache.get("a"));
        assertTrue(simpleCache.containsKey("b"));
        assertTrue(simpleCache.containsKey("c"));
        assertEquals(3, testingCacheLoader.countDataGenerator());
    }


    @Test(expected = IllegalStateException.class)
    public void loadAllWithErrorTest() {
        testingCacheLoader.doNextError();
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        simpleCache.loadAll(keys, true);
    }

    @Test
    public void loadAllWithExistingValuesTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        Set<String> keys = new HashSet<>(Arrays.asList("a","b","c"));
        simpleCache.loadAll(keys, false);
        assertEquals("A1", simpleCache.get("a"));
        assertTrue(simpleCache.containsKey("a"));
        assertTrue(simpleCache.containsKey("b"));
        assertTrue(simpleCache.containsKey("c"));
        assertEquals(2, testingCacheLoader.countDataGenerator());
    }

    @Test
    public void getTest() {
        SimpleCache simpleCache = createNewCache();
        String result = simpleCache.get("a");
        assertNotNull(result);
        assertEquals(1, testingCacheLoader.countDataGenerator());
    }


    @Test
    public void getWithDataTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        String result = simpleCache.get("a");
        assertNotNull(result);
        assertEquals(0, testingCacheLoader.countDataGenerator());
    }

    @Test(expected = IllegalStateException.class)
    public void getWithErrorTest() {
        testingCacheLoader.doNextError();
        SimpleCache simpleCache = createNewCache();
        simpleCache.get("a");
    }


    private static class TestingCacheLoader implements CacheLoader {

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
        public String load(String key) {
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
        public Map<String, String> loadAll(Iterable<String> keys) {
            Map<String, String> results = new HashMap<>();
            keys.forEach( key -> results.put(key, load(key)));
            return results;
        }

        private synchronized void doWait() {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextLong(50));
            } catch (InterruptedException e) {
                // Eat this
            }
        }
    }

}
