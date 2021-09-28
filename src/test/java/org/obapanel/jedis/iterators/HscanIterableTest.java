package org.obapanel.jedis.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.obapanel.jedis.iterators.MockOfJedis.unitTestEnabled;

public class HscanIterableTest {

    private static final Logger LOG = LoggerFactory.getLogger(HscanIterableTest.class);



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
        LOG.debug("before count {} for name {} with letters {}", count.get(), hscanitName, letters );
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


}
