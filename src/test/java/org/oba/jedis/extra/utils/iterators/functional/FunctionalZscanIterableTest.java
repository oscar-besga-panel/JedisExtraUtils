package org.oba.jedis.extra.utils.iterators.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.iterators.ZScanIterable;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.resps.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.oba.jedis.extra.utils.iterators.ScanUtil.deleteListOfKeysStartsWith;

public class FunctionalZscanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalZscanIterableTest.class);

    private static final String COMMON_REDIS_TEST_NAME = "zscanIterable:" + FunctionalZscanIterableTest.class.getName() + ":";

    private static AtomicInteger count = new AtomicInteger(0);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String zscanitName;
    private List<String> letters;
    private RedisClient redisClient;


    static double valueFromChar(String s) {
        if (s.isEmpty()) {
            return 0.0;
        } else {
            return Character.getNumericValue(s.charAt(0)) * 1.0;
        }
    }

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        zscanitName = COMMON_REDIS_TEST_NAME + System.currentTimeMillis() + ":" + count.incrementAndGet();
        redisClient = jtfTest.createRedisClient();
        letters = jtfTest.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), zscanitName, letters );
    }

    @After
    public void after() {
        if (redisClient != null) {
            redisClient.del(zscanitName);
            deleteListOfKeysStartsWith(redisClient, COMMON_REDIS_TEST_NAME);
            redisClient.close();
        }
    }

    void createABCData() {
        letters.forEach( letter -> {
            redisClient.zadd(zscanitName, valueFromChar(letter), letter);
        });
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  "*");
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
        assertEquals(zscanitName, zscanIterable.getName());
    }

    @Test
    public void iteratorEmpty2Test() {
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  50);
        List<Tuple> data = zscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorEmpty3Test() {
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName);
        List<Tuple> data = zscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorWithResultsTest() {
        int num = 0;
        createABCData();
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  "*");
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
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  "*");
        Iterator<Tuple> iterator =  zscanIterable.iterator();
        while(iterator.hasNext()) {
            assertTrue( redisClient.exists(zscanitName));
            assertNotNull( redisClient.zscore(zscanitName, iterator.next().getElement()));
        }
        assertNull(redisClient.zscore(zscanitName, "x0x"));
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  "*");
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
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  "*");
        zscanIterable.forEach( key -> {
            assertTrue( redisClient.exists(zscanitName));
            assertNotNull( redisClient.zscore(zscanitName, key.getElement()));
        });
        assertNull(redisClient.zscore(zscanitName, "a0b"));
    }


    @Test
    public void iteratorRemoveForEach1Test() {
        createABCData();
        redisClient.zadd(zscanitName, 1.1, "extra");
        List<String> deleted = new ArrayList<>();
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  "*");
        Iterator<Tuple> iterator = zscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next().getElement());
            iterator.remove();
        }
        deleted.forEach( key -> {
            assertNull(redisClient.zscore(zscanitName, key));
        });
    }

    @Test
    public void iteratorRemoveForEach2Test() {
        createABCData();
        List<String> deleted = new ArrayList<>();
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName,  "*");
        Iterator<Tuple> iterator = zscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next().getElement());
            iterator.remove();
        }
        deleted.forEach( key -> {
            assertNull(redisClient.zscore(zscanitName, key));
        });
    }

    @Test
    public void asListTest() {
        createABCData();
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName);
        List<Tuple> data = zscanIterable.asList();
        data.forEach(  tuple -> {
            assertTrue(letters.contains(tuple.getElement()));
        });
        assertEquals(letters.size(), data.size());
    }

    @Test(expected = IllegalStateException.class)
    public void errorInDeleteTest() {
        ZScanIterable zscanIterable = new ZScanIterable(redisClient, zscanitName, 20);
        Iterator<Tuple> iterator = zscanIterable.iterator();
        iterator.remove();
    }

}
