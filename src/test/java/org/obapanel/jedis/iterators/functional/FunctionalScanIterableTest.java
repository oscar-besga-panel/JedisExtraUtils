package org.obapanel.jedis.iterators.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.iterators.ScanIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FunctionalScanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalScanIterableTest.class);

    private static AtomicInteger count = new AtomicInteger(0);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String scanitName;
    private List<String> letters;
    private JedisPool jedisPool;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        scanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        jedisPool = jtfTest.createJedisPool();
        letters = jtfTest.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), scanitName, letters );
    }

    @After
    public void after() {
        if (jedisPool != null) {
            try(Jedis jedis = jedisPool.getResource()) {
                letters.forEach( letter -> {
                    jedis.del(scanitName + ":" + letter);
                });
            }
            jedisPool.close();
        }
    }

    void createABCData() {
        try(Jedis jedis = jedisPool.getResource()) {
            letters.forEach( letter -> {
                jedis.set(scanitName + ":" + letter, letter);
            });
        }
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        ScanIterable scanIterable = new ScanIterable(jedisPool,scanitName + ":*");
        Iterator<String> iterator =  scanIterable.iterator();
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
        ScanIterable scanIterable = new ScanIterable(jedisPool,scanitName + ":*");
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
        ScanIterable scanIterable = new ScanIterable(jedisPool,scanitName + ":*");
        Iterator<String> iterator =  scanIterable.iterator();
        while(iterator.hasNext()) {
            try(Jedis jedis = jedisPool.getResource()) {
                assertTrue( jedis.exists(iterator.next()));
            }
        }
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        ScanIterable scanIterable = new ScanIterable(jedisPool,scanitName + ":*");
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
        ScanIterable scanIterable = new ScanIterable(jedisPool,scanitName + ":*");
        scanIterable.forEach( key -> {
            try(Jedis jedis = jedisPool.getResource()) {
                assertTrue( jedis.exists(key));
            }
        });
    }


    @Test
    public void iteratorRemoveForEachTest() {
        createABCData();
        List<String> deleted = new ArrayList<>();
        ScanIterable scanIterable = new ScanIterable(jedisPool,scanitName + ":*");
        Iterator<String> iterator = scanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next());
            iterator.remove();
        }
        deleted.forEach( key -> {
            try(Jedis jedis = jedisPool.getResource()) {
                assertFalse( jedis.exists(key));
            }
        });
    }

    @Test
    public void asListTest() {
        createABCData();
        ScanIterable scanIterable = new ScanIterable(jedisPool, scanitName + ":*");
        List<String> data = scanIterable.asList();
        data.forEach( key -> {
            String value = get(key);
            assertTrue(letters.contains(value));
        });
        assertEquals(letters.size(), data.size());
    }

    private String get(String key) {
        try(Jedis jedis = jedisPool.getResource()){
            return jedis.get(key);
        }
    }

}
