package org.obapanel.jedis.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.obapanel.jedis.iterators.MockOfJedis.unitTestEnabled;

public class ZscanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZscanIterableTest.class);



    private static AtomicInteger count = new AtomicInteger(0);

    private MockOfJedis mockOfJedis;
    private String zscanitName;
    private List<String> letters;


    static double valueFromChar(String s) {
        if (s.isEmpty()) {
            return 0.0;
        } else {
            return Character.getNumericValue(s.charAt(0)) * 1.0;
        }
    }

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        zscanitName = "zscanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        mockOfJedis = new MockOfJedis();
        letters = mockOfJedis.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), zscanitName, letters );
    }

    @After
    public void after() {
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            jedis.del(zscanitName);
        }
        mockOfJedis.clearData();
    }

    void createABCData() {
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            letters.forEach( letter -> {
                jedis.zadd(zscanitName, valueFromChar(letter), letter);
            });
        }
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        ZScanIterable zscanIterable = new ZScanIterable(mockOfJedis.getJedisPool(), zscanitName,  "*");
        Iterator<Tuple> iterator =  zscanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            Tuple next = iterator.next();
            sb.append(next.getElement() + ":" + next.getScore());
        }
        assertNotNull(iterator);
        assertTrue(sb.length() == 0);
        assertTrue(num == 0);
    }


    @Test
    public void iteratorWithResultsTest() {
        int num = 0;
        createABCData();
        ZScanIterable zscanIterable = new ZScanIterable(mockOfJedis.getJedisPool(), zscanitName,  "*");
        Iterator<Tuple> iterator =  zscanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            Tuple next = iterator.next();
            sb.append(next.getElement() + ":" + next.getScore());
        }
        assertNotNull(iterator);
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(letter + ":" + valueFromChar(letter)) >= 0);
        });
        assertTrue(num == letters.size());
    }

    @Test
    public void iteratorWithResultKeysTest() {
        createABCData();
        ZScanIterable zscanIterable = new ZScanIterable(mockOfJedis.getJedisPool(), zscanitName,  "*");
        Iterator<Tuple> iterator =  zscanIterable.iterator();
        while(iterator.hasNext()) {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertTrue( jedis.exists(zscanitName));
                assertNotNull( jedis.zscore(zscanitName, iterator.next().getElement()));
            }
        }
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            assertNull(jedis.zscore(zscanitName, "x0x"));
        }
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        ZScanIterable zscanIterable = new ZScanIterable(mockOfJedis.getJedisPool(), zscanitName,  "*");
        zscanIterable.forEach( element -> {
            num.incrementAndGet();
            sb.append(element.getElement() + ":" + element.getScore());
        });
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(letter + ":" + valueFromChar(letter)) >= 0);
        });
        assertTrue(num.get() == letters.size());
    }

    @Test
    public void iteratorWithResultKeysForEachTest() {
        createABCData();
        ZScanIterable zscanIterable = new ZScanIterable(mockOfJedis.getJedisPool(), zscanitName,  "*");
        zscanIterable.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertTrue( jedis.exists(zscanitName));
                assertNotNull( jedis.zscore(zscanitName, key.getElement()));
            }
        });
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            assertNull(jedis.zscore(zscanitName, "a0b"));
        }
    }


    @Test
    public void iteratorRemoveForEach1Test() {
        createABCData();
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            jedis.zadd(zscanitName, 1.1, "extra");
        }
        List<String> deleted = new ArrayList<>();
        ZScanIterable zscanIterable = new ZScanIterable(mockOfJedis.getJedisPool(), zscanitName,  "*");
        Iterator<Tuple> iterator = zscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next().getElement());
            iterator.remove();
        }
        deleted.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertNull(jedis.zscore(zscanitName, key));
            }
        });
    }

    @Test
    public void iteratorRemoveForEach2Test() {
        createABCData();
        List<String> deleted = new ArrayList<>();
        ZScanIterable zscanIterable = new ZScanIterable(mockOfJedis.getJedisPool(), zscanitName,  "*");
        Iterator<Tuple> iterator = zscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next().getElement());
            iterator.remove();
        }
        deleted.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertNull(jedis.zscore(zscanitName, key));
            }
        });
    }

}
