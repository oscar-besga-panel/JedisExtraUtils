package org.obapanel.jedis.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.obapanel.jedis.iterators.MockOfJedis.unitTestEnabled;

public class HscanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HscanIterableTest.class);



    private static AtomicInteger count = new AtomicInteger(0);


    private String hscanitName;
    private List<String> letters;
    private MockOfJedis mockOfJedis;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        hscanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        mockOfJedis = new MockOfJedis();
        letters = mockOfJedis.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), hscanitName, letters );
    }

    @After
    public void after() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }


    void createABCData() {
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            letters.forEach( letter -> {
                jedis.hset(hscanitName, letter, letter);
            });
        }
    }


    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(), hscanitName , "*");
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
    }

    @Test
    public void iteratorWithResultsTest() {
        createABCData();
        int num = 0;
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(), hscanitName , "*");
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
    public void iteratorEmpty2Test() {
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(), hscanitName , 20);
        List<Map.Entry<String,String>> data = hscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorEmpty3Test() {
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(), hscanitName);
        List<Map.Entry<String,String>> data = hscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorEmpty4Test() {
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(), hscanitName);
        Map<String, String> data = hscanIterable.asMap();
        assertTrue(data.isEmpty());
    }


    @Test
    public void iteratorWithResultKeysTest() {
        createABCData();
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(),hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator =  hscanIterable.iterator();
        while(iterator.hasNext()) {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                Map.Entry<String,String> entry = iterator.next();
                assertEquals( entry.getValue(), jedis.hget(hscanitName, entry.getKey()));
            }
        }
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(),hscanitName,"*");
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
        HScanIterable scanIterable = new HScanIterable(mockOfJedis.getJedisPool(),hscanitName, "*");
        scanIterable.forEach( entry -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertEquals( entry.getValue(), jedis.hget(hscanitName, entry.getKey()));
            }
        });
    }


    @Test
    public void iteratorRemoveForEachTest() {
        createABCData();
        List<String> deletedKeys = new ArrayList<>();
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(),hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator = hscanIterable.iterator();
        while (iterator.hasNext()) {
            deletedKeys.add(iterator.next().getKey());
            iterator.remove();
        }
        deletedKeys.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertNull( jedis.hget(hscanitName, key));
            }
        });
    }

    @Test
    public void asListTest() {
        createABCData();
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(),hscanitName, "*");
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
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(),hscanitName, "*");
        Map<String, String> data = hscanIterable.asMap();
        letters.forEach( letter -> {
            assertTrue(data.containsKey(letter));
            assertTrue(data.get(letter).equals(letter));
        });
        assertEquals(letters.size(), data.size());
    }

    @Test(expected = IllegalStateException.class)
    public void errorInDeleteTest() {
        HScanIterable hscanIterable = new HScanIterable(mockOfJedis.getJedisPool(),hscanitName, 50);
        Iterator<Map.Entry<String,String>> iterator = hscanIterable.iterator();
        iterator.remove();
    }

}
