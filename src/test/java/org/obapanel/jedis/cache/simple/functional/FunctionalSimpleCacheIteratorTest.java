package org.obapanel.jedis.cache.simple.functional;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.obapanel.jedis.cache.simple.SimpleCache;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.utils.SimpleEntry;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class FunctionalSimpleCacheIteratorTest {



    private final JedisTestFactory jtfTest = JedisTestFactory.get();


    private JedisPool jedisPool;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void tearDown() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    SimpleCache createNewCache() {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        return new SimpleCache(jedisPool, name, 3_600_000);
    }

    @Test
    public void getIteratorTest() {
        SimpleCache simpleCache = createNewCache();
        assertNotNull(simpleCache.iterator());
    }

    @Test
    public void iteratorTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
        int count = 0;
        Iterator<Map.Entry<String, String>> it = simpleCache.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            assertTrue(data.containsKey(entry.getKey()));
            assertTrue(data.containsValue(entry.getValue()));
            count++;
        }
        assertEquals(3, count);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.iterator().remove();
    }

    @Test
    public void forEachRemainingTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
        AtomicInteger count = new AtomicInteger(0);
        simpleCache.iterator().forEachRemaining( entry -> {
            assertTrue(data.containsKey(entry.getKey()));
            assertTrue(data.containsValue(entry.getValue()));
            count.incrementAndGet();
        });
        assertEquals(3, count.get());
    }

    @Test
    public void entryTest(){
        SimpleEntry entry = new SimpleEntry("a", "A1");
        assertEquals("a", entry.getKey());
        assertEquals("A1", entry.getValue());
        assertTrue(entry.toString().contains("a"));
        assertTrue(entry.toString().contains("A1"));
        SimpleEntry entry2 = new SimpleEntry("a", "A1");
        assertEquals(entry2, entry);
        assertEquals(entry2.hashCode(), entry.hashCode());
        assertEquals(entry, entry2);
        SimpleEntry entry3 = new SimpleEntry("b", "B1");
        assertNotEquals(entry3, entry);
        assertNotEquals(entry3.hashCode(), entry.hashCode());
        assertNotEquals(entry, entry3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void entryUnsupportedTest(){
        SimpleEntry entry = new SimpleEntry("a", "A1");
        entry.setValue("A2");
    }

    @Test(expected = NoSuchElementException.class)
    public void iteratorErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.iterator().next();
    }

    @Test
    public void iteratorAsListTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
        List<Map.Entry<String,String>> list = simpleCache.iterator().asList();
        assertEquals(3, list.size());
        for(Map.Entry<String,String> entry: list) {
            assertTrue(Arrays.asList("a","b","c").contains(entry.getKey()));
            assertTrue(Arrays.asList("A1","B1","C1").contains(entry.getValue()));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iteratorAsListErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        List<Map.Entry<String,String>> list = simpleCache.iterator().asList();
        list.add(new SimpleEntry("b", "B1"));
    }


    @Test
    public void iteratorAsMapTest() {
        SimpleCache simpleCache = createNewCache();
        Map<String, String> data = new HashMap<>();
        data.put("a", "A1");
        data.put("b", "B1");
        data.put("c", "C1");
        simpleCache.putAll(data);
        Map<String, String> map = simpleCache.iterator().asMap();
        assertEquals(3, map.size());
        assertEquals("A1", map.get("a"));
        assertEquals("B1", map.get("b"));
        assertEquals("C1", map.get("c"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void iteratorAsMapErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.put("a", "A1");
        Map<String, String> map = simpleCache.iterator().asMap();
        map.put("b","B1");
    }

}
