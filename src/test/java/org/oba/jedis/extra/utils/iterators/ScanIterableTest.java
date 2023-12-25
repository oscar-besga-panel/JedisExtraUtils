package org.oba.jedis.extra.utils.iterators;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.oba.jedis.extra.utils.iterators.MockOfJedis.unitTestEnabled;

public class ScanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanIterableTest.class);


    private static AtomicInteger count = new AtomicInteger(0);


    private String scanitName;
    private List<String> letters;
    private MockOfJedis mockOfJedis;



    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        scanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        mockOfJedis = new MockOfJedis();
        letters = mockOfJedis.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), scanitName, letters );
    }

    @After
    public void after() {
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            letters.forEach( letter -> {
                jedis.del(scanitName + ":" + letter);
            });
        }
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    void createABCData() {
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            letters.forEach( letter -> {
                jedis.set(scanitName + ":" + letter, letter);
            });
        }
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        Iterator<String> iterator =  scanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            sb.append(iterator.next());
        }
        assertNotNull(iterator);
        assertTrue(sb.length() == 0);
        assertTrue(num == 0);
        assertNotNull(scanIterable.getJedisPool());
    }

    @Test
    public void iteratorEmpty2Test() {
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        List<String> data = scanIterable.asList();
        assertTrue(data.isEmpty());
    }


    @Test
    public void iteratorWithResultsTest() {
        int num = 0;
        createABCData();
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        Iterator<String> iterator =  scanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            sb.append(iterator.next());
        }
        assertNotNull(iterator);
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(scanitName + ":" + letter) >= 0);
        });
        assertTrue(num == letters.size());
    }

    @Test
    public void iteratorWithResultKeysTest() {
        createABCData();
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        Iterator<String> iterator =  scanIterable.iterator();
        while(iterator.hasNext()) {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertTrue( jedis.exists(iterator.next()));
            }
        }
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        scanIterable.forEach( key -> {
            num.incrementAndGet();
            sb.append(key);
        });
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(scanitName + ":" + letter) >= 0);
        });
        assertTrue(num.get() == letters.size());
    }

    @Test
    public void iteratorWithResultKeysForEachTest() {
        createABCData();
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        scanIterable.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertTrue( jedis.exists(key));
            }
        });
    }


    @Test
    public void iteratorRemoveForEachTest() {
        createABCData();
        int originalSize = letters.size();
        List<String> deleted = new ArrayList<>();
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        Iterator<String> iterator = scanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add(iterator.next());
            iterator.remove();
        }
        scanIterable.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertFalse( jedis.exists(key));
            }
        });
        assertEquals(originalSize, deleted.size());

    }

    @Test
    public void asListTest() {
        createABCData();
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool(),scanitName + ":*");
        List<String> data = scanIterable.asList();
        data.forEach( key -> {
            String value = mockOfJedis.getCurrentData().get(key);
            assertTrue(letters.contains(value));
        });
        assertEquals(letters.size(), data.size());
    }

    @Test(expected = IllegalStateException.class)
    public void errorInDeleteTest() {
        ScanIterable scanIterable = new ScanIterable(mockOfJedis.getJedisPool());
        Iterator<String> iterator = scanIterable.iterator();
        iterator.remove();
    }



}
