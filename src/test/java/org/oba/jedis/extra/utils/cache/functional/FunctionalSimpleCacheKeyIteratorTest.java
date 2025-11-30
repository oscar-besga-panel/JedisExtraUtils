package org.oba.jedis.extra.utils.cache.functional;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.oba.jedis.extra.utils.cache.CacheKeyIterator;
import org.oba.jedis.extra.utils.cache.SimpleCache;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import redis.clients.jedis.JedisPooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.Silent.class)
public class FunctionalSimpleCacheKeyIteratorTest {

    private static final List<String> listNameKeysToDelete = new ArrayList<>();


    private final JedisTestFactory jtfTest = JedisTestFactory.get();


    private JedisPooled jedisPooled;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        jedisPooled = jtfTest.createJedisPooled();
    }

    @After
    public void tearDown() {
        if (jedisPooled != null) {
            listNameKeysToDelete.forEach(k -> jedisPooled.del(k));
            jedisPooled.close();
        }
    }

    SimpleCache createNewCache() {
        String name = "cache:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        listNameKeysToDelete.add(name);
        return new SimpleCache(jedisPooled, name, 3_600_000);
    }

    @Test
    public void getKeyIteratorTest() {
        SimpleCache simpleCache = createNewCache();
        assertNotNull(simpleCache.keysIterator());
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
        Iterator<String> kit = simpleCache.keysIterator();
        while (kit.hasNext()) {
            String key = kit.next();
            String value = ((CacheKeyIterator)kit).nextValue();
            assertTrue(data.containsKey(key));
            assertTrue(data.containsValue(value));
            count++;
        }
        assertEquals(3, count);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.keysIterator().remove();
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
        simpleCache.keysIterator().forEachRemaining( key -> {
            assertTrue(data.containsKey(key));
            assertTrue(data.containsValue(simpleCache.get(key)));
            count.incrementAndGet();
        });
        assertEquals(3, count.get());
    }

    @Test(expected = NoSuchElementException.class)
    public void iteratorKeyErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.keysIterator().next();
    }

    @Test(expected = NoSuchElementException.class)
    public void iteratorValueErrorTest() {
        SimpleCache simpleCache = createNewCache();
        simpleCache.keysIterator().nextValue();
    }


}
