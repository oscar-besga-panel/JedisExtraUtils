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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.obapanel.jedis.iterators.MockOfJedis.unitTestEnabled;

public class SscanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SscanIterableTest.class);



    private static AtomicInteger count = new AtomicInteger(0);


    private MockOfJedis mockOfJedis;
    private String sscanitName;
    private List<String> letters;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabled());
        if (!unitTestEnabled()) return;
        sscanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        mockOfJedis = new MockOfJedis();
        letters = mockOfJedis.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), sscanitName, letters );
    }

    @After
    public void after() {
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            jedis.del(sscanitName);
        }
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
    }

    void createABCData() {
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            letters.forEach( letter -> {
                jedis.sadd(sscanitName, letter);
            });
        }
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(),sscanitName,  "*");
        Iterator<String> iterator =  sscanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            sb.append(iterator.next());
        }
        assertNotNull(iterator);
        assertTrue(sb.length() == 0);
        assertTrue(num == 0);
    }


    @Test
    public void iteratorWithResultsTest() {
        int num = 0;
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(),sscanitName,  "*");
        Iterator<String> iterator =  sscanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            sb.append(iterator.next());
        }
        assertNotNull(iterator);
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(letter) >= 0);
        });
        assertTrue(num == letters.size());
    }

    @Test
    public void iteratorWithResultKeysTest() {
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(), sscanitName, "*");
        Iterator<String> iterator =  sscanIterable.iterator();
        while(iterator.hasNext()) {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertTrue( jedis.exists(sscanitName));
                assertTrue( jedis.sismember(sscanitName, iterator.next()));
            }
        }
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(), sscanitName, "*");
        sscanIterable.forEach( key -> {
            num.incrementAndGet();
            sb.append(key);
        });
        assertFalse(sb.length() == 0);
        letters.forEach( letter -> {
            assertTrue(sb.indexOf(letter) >= 0);
        });
        assertTrue(num.get() == letters.size());
    }

    @Test
    public void iteratorWithResultKeysForEachTest() {
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(), sscanitName, "*");
        sscanIterable.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertTrue( jedis.exists(sscanitName));
                assertTrue( jedis.sismember(sscanitName, key));
            }
        });
    }


    @Test
    public void iteratorRemoveForEach1Test() {
        createABCData();
        try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
            jedis.sadd(sscanitName, "extra");
        }
        List<String> deleted = new ArrayList<>();
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(), sscanitName, "*");
        Iterator<String> iterator = sscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next());
            iterator.remove();
        }
        deleted.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertFalse( jedis.sismember(sscanitName, key));
            }
        });
    }

    @Test
    public void iteratorRemoveForEach2Test() {
        createABCData();
        List<String> deleted = new ArrayList<>();
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(), sscanitName, "*");
        Iterator<String> iterator = sscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next());
            iterator.remove();
        }
        deleted.forEach( key -> {
            try(Jedis jedis = mockOfJedis.getJedisPool().getResource()) {
                assertFalse( jedis.sismember(sscanitName, key));
            }
        });
    }

    @Test
    public void asListTest() {
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(mockOfJedis.getJedisPool(), sscanitName);
        List<String> data = sscanIterable.asList();
        letters.forEach( letter -> {
           assertTrue(data.contains(letter));
        });
        assertEquals(letters.size(), data.size());
    }


}
