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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FunctionalScanIterableTest {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionalScanIterableTest.class);

    private static AtomicInteger count = new AtomicInteger(0);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String scanitName;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        scanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        jedisPool = jtfTest.createJedisPool();
        LOG.debug("before count {} for name", count.get(), scanitName );
    }

    @After
    public void after() {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.del(scanitName + ":a");
            jedis.del(scanitName + ":b");
            jedis.del(scanitName + ":c");
            jedis.del(scanitName + ":d");
            jedis.del(scanitName + ":e");
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    void createABCData() {
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.set(scanitName + ":a", "a");
            jedis.set(scanitName + ":b", "b");
            jedis.set(scanitName + ":c", "c");
            jedis.set(scanitName + ":d", "d");
            jedis.set(scanitName + ":e", "e");
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
        assertTrue(sb.indexOf(scanitName + ":a") >= 0);
        assertTrue(sb.indexOf(scanitName + ":b") >= 0);
        assertTrue(sb.indexOf(scanitName + ":c") >= 0);
        assertTrue(sb.indexOf(scanitName + ":d") >= 0);
        assertTrue(sb.indexOf(scanitName + ":e") >= 0);
        assertTrue(num == 5);
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
        assertTrue(sb.indexOf(scanitName + ":a") >= 0);
        assertTrue(sb.indexOf(scanitName + ":b") >= 0);
        assertTrue(sb.indexOf(scanitName + ":c") >= 0);
        assertTrue(sb.indexOf(scanitName + ":d") >= 0);
        assertTrue(sb.indexOf(scanitName + ":e") >= 0);
        assertTrue(num.get() == 5);
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


}
