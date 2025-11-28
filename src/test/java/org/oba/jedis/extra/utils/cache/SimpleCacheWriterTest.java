package org.oba.jedis.extra.utils.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.oba.jedis.extra.utils.utils.SimpleEntry;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;

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
import static org.oba.jedis.extra.utils.cache.MockOfJedisForSimpleCache.unitTestEnabledForSimpleCache;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class })
public class SimpleCacheWriterTest {

    private MockOfJedisForSimpleCache mockOfJedisForSimpleCache;

    private final TestingCacheWriter testingCacheWriter = new TestingCacheWriter();

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(unitTestEnabledForSimpleCache());
        if (!unitTestEnabledForSimpleCache()) return;
        mockOfJedisForSimpleCache = new MockOfJedisForSimpleCache();
    }

    @After
    public void tearDown() {
        if (mockOfJedisForSimpleCache != null) {
            mockOfJedisForSimpleCache.clearData();
        }
    }

    SimpleCache createNewCache() {
        return createNewCache(testingCacheWriter);
    }

    SimpleCache createNewCache(CacheWriter cacheWriter) {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        return new SimpleCache(mockOfJedisForSimpleCache.getJedisPooled(), name, 3_600_000)
                .withCacheWriter(cacheWriter);
    }

    @Test
    public void putTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void putWithErrorTest() {
        SimpleCache simpleCache = createNewCache();
        testingCacheWriter.doNextError();
        simpleCache.put("a","A1");
    }

    @Test
    public void getAndPutTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        String oldResult = simpleCache.getAndPut("a", "A2");
        assertEquals("A1", oldResult);
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void putAllTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        assertEquals("B1", simpleCache.get("b"));
        assertEquals("B1", testingCacheWriter.get("b"));
        assertEquals("C1", simpleCache.get("c"));
        assertEquals("C1", testingCacheWriter.get("c"));
        assertEquals(3, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void defaultPutAllTest() {
        SimpleCache simpleCache = createNewCache(new TestingDefaultsCacheWriter(testingCacheWriter));
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        assertEquals("B1", simpleCache.get("b"));
        assertEquals("B1", testingCacheWriter.get("b"));
        assertEquals("C1", simpleCache.get("c"));
        assertEquals("C1", testingCacheWriter.get("c"));
        assertEquals(3, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void putAllWithErrorTest() {
        SimpleCache simpleCache = createNewCache();
        testingCacheWriter.doNextError();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
    }

    @Test
    public void putIfAbsentTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
        boolean putResult1 = simpleCache.putIfAbsent("d","D2");
        boolean putResult2 = simpleCache.putIfAbsent("a","A2");
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        assertEquals("B1", simpleCache.get("b"));
        assertEquals("B1", testingCacheWriter.get("b"));
        assertEquals("C1", simpleCache.get("c"));
        assertEquals("C1", testingCacheWriter.get("c"));
        assertEquals("D2", simpleCache.get("d"));
        assertEquals("D2", testingCacheWriter.get("d"));
        assertTrue(putResult1);
        assertFalse(putResult2);
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void removeTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        boolean result = simpleCache.remove("a");
        assertTrue(result);
        assertFalse(simpleCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(1, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void removeWithErrorTest() {
        SimpleCache simpleCache = createNewCache();
        testingCacheWriter.doNextError();
        simpleCache.put("a","A1");
        simpleCache.remove("a");
    }


    @Test
    public void removeWithValueTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        boolean result1 = simpleCache.remove("a", "A2");
        assertFalse(result1);
        assertTrue(simpleCache.containsKey("a"));
        assertNotNull(testingCacheWriter.get("a"));
        boolean result2 = simpleCache.remove("a", "A1");
        assertTrue(result2);
        assertFalse(simpleCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(1, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void getAndRemoveTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        String oldResult = simpleCache.getAndRemove("a");
        assertEquals("A1", oldResult);
        assertFalse(simpleCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertEquals(1, testingCacheWriter.countDataInserted());
        assertEquals(1, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void replaceTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        boolean result = simpleCache.replace("a", "A2");
        assertTrue(result);
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void replaceWithValueTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a","A1");
        boolean result1 = simpleCache.replace("a", "A3", "A2");
        assertFalse(result1);
        assertEquals("A1", simpleCache.get("a"));
        assertEquals("A1", testingCacheWriter.get("a"));
        boolean result2 = simpleCache.replace("a", "A1", "A2");
        assertTrue(result2);
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void getAndReplaceTest() {
        SimpleCache simpleCache = createNewCache();
        String result1 = simpleCache.getAndReplace("a","A2");
        assertNull(result1);
        assertNull( simpleCache.get("a"));
        assertNull( testingCacheWriter.get("a"));
        simpleCache.put("a","A1");
        String result2 = simpleCache.getAndReplace("a","A2");
        assertEquals(result2, "A1");
        assertEquals("A2", simpleCache.get("a"));
        assertEquals("A2", testingCacheWriter.get("a"));
        assertEquals(2, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void removeAllTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        simpleCache.putAll(data);
        simpleCache.removeAll(new HashSet<>(Arrays.asList("a","b","c")));
        assertFalse(simpleCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertFalse(simpleCache.containsKey("b"));
        assertNull(testingCacheWriter.get("b"));
        assertFalse(simpleCache.containsKey("c"));
        assertNull(testingCacheWriter.get("c"));
        assertTrue(simpleCache.containsKey("d"));
        assertNotNull(testingCacheWriter.get("d"));
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(3, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void defaultRemoveAllTest() {
        SimpleCache simpleCache = createNewCache(new TestingDefaultsCacheWriter(testingCacheWriter));
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        simpleCache.putAll(data);
        simpleCache.removeAll(new HashSet<>(Arrays.asList("a","b","c")));
        assertFalse(simpleCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertFalse(simpleCache.containsKey("b"));
        assertNull(testingCacheWriter.get("b"));
        assertFalse(simpleCache.containsKey("c"));
        assertNull(testingCacheWriter.get("c"));
        assertTrue(simpleCache.containsKey("d"));
        assertNotNull(testingCacheWriter.get("d"));
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(3, testingCacheWriter.countDataDeleted());
    }

    @Test(expected = IllegalStateException.class)
    public void removeAllWithErrorTest() {
        SimpleCache SimpleCache = createNewCache();
        testingCacheWriter.doNextError();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        SimpleCache.putAll(data);
        SimpleCache.removeAll(new HashSet<>(Arrays.asList("a","b","c")));
    }

    @Test
    public void removeAllAllTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        simpleCache.putAll(data);
        simpleCache.removeAll();
        assertFalse(simpleCache.containsKey("a"));
        assertNull(testingCacheWriter.get("a"));
        assertFalse(simpleCache.containsKey("b"));
        assertNull(testingCacheWriter.get("b"));
        assertFalse(simpleCache.containsKey("c"));
        assertNull(testingCacheWriter.get("c"));
        assertFalse(simpleCache.containsKey("d"));
        assertNull(testingCacheWriter.get("d"));
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(4, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void clearTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        data.put("d", "D1");
        simpleCache.putAll(data);
        simpleCache.clear();
        assertFalse(simpleCache.containsKey("a"));
        assertNotNull(testingCacheWriter.get("a"));
        assertFalse(simpleCache.containsKey("b"));
        assertNotNull(testingCacheWriter.get("b"));
        assertFalse(simpleCache.containsKey("c"));
        assertNotNull(testingCacheWriter.get("c"));
        assertFalse(simpleCache.containsKey("d"));
        assertNotNull(testingCacheWriter.get("d"));
        assertEquals(4, testingCacheWriter.countDataInserted());
        assertEquals(0, testingCacheWriter.countDataDeleted());
    }

    @Test
    public void cacheWriterWriteTest() {
        TestingCacheWriter localCacheWriter = new TestingCacheWriter();
        localCacheWriter.write(new SimpleEntry("a","A1"));
        localCacheWriter.write("b","B1");
        Map<String, String> data = new HashMap<>();
        data.put("c","C1");
        data.put("d","D1");
        data.put("e","E1");
        localCacheWriter.writeAll(data);
        assertEquals("A1", localCacheWriter.get("a"));
        assertEquals("B1", localCacheWriter.get("b"));
        assertEquals("C1", localCacheWriter.get("c"));
        assertEquals("D1", localCacheWriter.get("d"));
        assertEquals("E1", localCacheWriter.get("e"));
        localCacheWriter.delete("d");
        assertNotNull(localCacheWriter.get("a"));
        assertNotNull(localCacheWriter.get("b"));
        assertNotNull(localCacheWriter.get("c"));
        assertNull(localCacheWriter.get("d"));
        assertNotNull(localCacheWriter.get("e"));
        localCacheWriter.deleteAll(Arrays.asList("b","c","e"));
        assertNotNull(localCacheWriter.get("a"));
        assertNull(localCacheWriter.get("b"));
        assertNull(localCacheWriter.get("c"));
        assertNull(localCacheWriter.get("d"));
        assertNull(localCacheWriter.get("e"));
    }

    private static class TestingCacheWriter implements CacheWriter {

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
        public void write(String key, String value) {
            doWaitOrError();
            dataInserted.incrementAndGet();
            internalData.put(key, value);
        }

        @Override
        public void writeAll(Map<String,String> values)  {
            values.forEach(this::write);
        }

        @Override
        public void delete(String key) {
            doWaitOrError();
            dataDeleted.incrementAndGet();
            internalData.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            keys.forEach(this::delete);
        }

        private synchronized void doWaitOrError() {
            if (launchErrorInWriter.get()) {
                throw new IllegalStateException("Test error");
            }
            try {
                Thread.sleep(ThreadLocalRandom.current().nextLong(10L));
            } catch (InterruptedException e) {
                // Eat this
            }
        }

    }

    private static class TestingDefaultsCacheWriter implements CacheWriter {

        private final TestingCacheWriter testingCacheWriter;

        public TestingDefaultsCacheWriter(TestingCacheWriter testingCacheWriter) {
            this.testingCacheWriter = testingCacheWriter;
        }

        @Override
        public void write(String key, String value) {
            testingCacheWriter.write(key, value);
        }

        @Override
        public void delete(String key) {
            testingCacheWriter.delete(key);
        }
    }


}
