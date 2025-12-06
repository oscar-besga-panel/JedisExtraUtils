package org.oba.jedis.extra.utils.iterators.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.iterators.HScanIterable;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FunctionalHscanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalHscanIterableTest.class);



    private static AtomicInteger count = new AtomicInteger(0);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String hscanitName;
    private List<String> letters;
    private JedisPooled jedisPooled;



    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        hscanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        jedisPooled = jtfTest.createJedisPooled();
        letters = jtfTest.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), hscanitName, letters );
    }

    @After
    public void after() {
        if (jedisPooled != null) {
            jedisPooled.del(hscanitName);
            jedisPooled.close();
        }
    }


    void createABCData() {
        letters.forEach( letter -> {
            jedisPooled.hset(hscanitName, letter, letter);
        });
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName , "*");
        Iterator<Map.Entry<String,String>> iterator =  hscanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            Map.Entry<String,String> entry = iterator.next();
            sb.append(entry.getKey() + ":" + entry.getValue());
        }
        assertNotNull(iterator);
        assertTrue(sb.length() == 0);
        assertTrue(num == 0);
        assertEquals(hscanitName, hscanIterable.getName());
    }

    @Test
    public void iteratorEmpty2Test() {
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName , 20);
        List<Map.Entry<String,String>> data = hscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorEmpty3Test() {
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName);
        List<Map.Entry<String,String>> data = hscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorEmpty4Test() {
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName);
        Map<String, String> data = hscanIterable.asMap();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorWithResultsTest() {
        createABCData();
        int num = 0;
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName , "*");
        Iterator<Map.Entry<String,String>> iterator =  hscanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            Map.Entry<String,String> entry = iterator.next();
            sb.append(entry.getKey() + ":" + entry.getValue());
        }
        assertNotNull(iterator);
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(letter + ":" + letter) >= 0);
        });
        assertTrue(num == letters.size());
    }

    @Test
    public void iteratorWithResultKeysTest() {
        createABCData();
        HScanIterable hscanIterable = new HScanIterable(jedisPooled,hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator =  hscanIterable.iterator();
        while(iterator.hasNext()) {
            assertTrue( jedisPooled.exists(hscanitName));
            Map.Entry<String,String> entry = iterator.next();
            assertEquals( entry.getValue(), jedisPooled.hget(hscanitName, entry.getKey()));
        }
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        HScanIterable hscanIterable = new HScanIterable(jedisPooled,hscanitName,"*");
        hscanIterable.forEach( entry -> {
            num.incrementAndGet();
            sb.append(entry.getKey() + ":" + entry.getValue());
        });
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(letter + ":" + letter) >= 0);
        });
        assertTrue(num.get() == letters.size());
    }

    @Test
    public void iteratorWithResultKeysForEachTest() {
        createABCData();
        HScanIterable scanIterable = new HScanIterable(jedisPooled,hscanitName, "*");
        scanIterable.forEach( entry -> {
            assertTrue( jedisPooled.exists(hscanitName));
            assertEquals( entry.getValue(), jedisPooled.hget(hscanitName, entry.getKey()));
        });
    }


    @Test
    public void iteratorRemoveForEach1Test() {
        createABCData();
        jedisPooled.hset(hscanitName, "extra", "extra");
        List<String> deletedKeys = new ArrayList<>();
        HScanIterable hscanIterable = new HScanIterable(jedisPooled,hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator = hscanIterable.iterator();
        while (iterator.hasNext()) {
            deletedKeys.add(iterator.next().getKey());
            iterator.remove();
        }
        deletedKeys.forEach( key -> {
            assertNull( jedisPooled.hget(hscanitName, key));
        });
    }

    @Test
    public void iteratorRemoveForEach2Test() {
        createABCData();
        List<String> deletedKeys = new ArrayList<>();
        HScanIterable hscanIterable = new HScanIterable(jedisPooled,hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator = hscanIterable.iterator();
        while (iterator.hasNext()) {
            deletedKeys.add(iterator.next().getKey());
            iterator.remove();
        }
        deletedKeys.forEach( key -> {
            assertNull( jedisPooled.hget(hscanitName, key));
        });
    }

    @Test
    public void asListTest() {
        createABCData();
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName, "*");
        List<Map.Entry<String,String>> data = hscanIterable.asList();
        Map<String, String> dataMap = new HashMap<>();
        data.forEach( e -> dataMap.put(e.getKey(), e.getValue()));
        letters.forEach( letter -> {
            assertTrue(dataMap.containsKey(letter));
            assertTrue(dataMap.get(letter).equals(letter));
        });
        assertEquals(letters.size(), data.size());
    }

    @Test
    public void asMapTest() {
        createABCData();
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName, "*");
        Map<String, String> data = hscanIterable.asMap();
        letters.forEach( letter -> {
            assertTrue(data.containsKey(letter));
            assertTrue(data.get(letter).equals(letter));
        });
        assertEquals(letters.size(), data.size());
    }

    @Test(expected = IllegalStateException.class)
    public void errorInDeleteTest() {
        HScanIterable hscanIterable = new HScanIterable(jedisPooled, hscanitName, 50);
        Iterator<Map.Entry<String,String>> iterator = hscanIterable.iterator();
        iterator.remove();
    }

}
