package org.obapanel.jedis.iterators.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.iterators.HScanIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FunctionalHscanIterableTest {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionalHscanIterableTest.class);



    private static AtomicInteger count = new AtomicInteger(0);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String hscanitName;
    private List<String> letters;
    private JedisPool jedisPool;



    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        hscanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        jedisPool = jtfTest.createJedisPool();
        letters = jtfTest.randomSizedListOfChars();
        LOG.debug("before count {} for name {} with letters {}", count.get(), hscanitName, letters );
    }

    @After
    public void after() {
        if (jedisPool != null) {
            try(Jedis jedis = jedisPool.getResource()) {
                jedis.del(hscanitName);
            }
            jedisPool.close();
        }
    }


    void createABCData() {
        try(Jedis jedis = jedisPool.getResource()) {
            letters.forEach( letter -> {
                jedis.hset(hscanitName, letter, letter);
            });
        }
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        HScanIterable hscanIterable = new HScanIterable(jedisPool, hscanitName , "*");
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
        HScanIterable hscanIterable = new HScanIterable(jedisPool, hscanitName , "*");
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
        HScanIterable hscanIterable = new HScanIterable(jedisPool,hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator =  hscanIterable.iterator();
        while(iterator.hasNext()) {
            try(Jedis jedis = jedisPool.getResource()) {
                assertTrue( jedis.exists(hscanitName));
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
        HScanIterable hscanIterable = new HScanIterable(jedisPool,hscanitName,"*");
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
        HScanIterable scanIterable = new HScanIterable(jedisPool,hscanitName, "*");
        scanIterable.forEach( entry -> {
            try(Jedis jedis = jedisPool.getResource()) {
                assertTrue( jedis.exists(hscanitName));
                assertEquals( entry.getValue(), jedis.hget(hscanitName, entry.getKey()));
            }
        });
    }


    @Test
    public void iteratorRemoveForEach1Test() {
        createABCData();
        try(Jedis jedis = jedisPool.getResource()) {
            jedis.hset(hscanitName, "extra", "extra");
        }
        List<String> deletedKeys = new ArrayList<>();
        HScanIterable hscanIterable = new HScanIterable(jedisPool,hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator = hscanIterable.iterator();
        while (iterator.hasNext()) {
            deletedKeys.add(iterator.next().getKey());
            iterator.remove();
        }
        deletedKeys.forEach( key -> {
            try(Jedis jedis = jedisPool.getResource()) {
                assertNull( jedis.hget(hscanitName, key));
            }
        });
    }

    @Test
    public void iteratorRemoveForEach2Test() {
        createABCData();
        List<String> deletedKeys = new ArrayList<>();
        HScanIterable hscanIterable = new HScanIterable(jedisPool,hscanitName, "*");
        Iterator<Map.Entry<String,String>> iterator = hscanIterable.iterator();
        while (iterator.hasNext()) {
            deletedKeys.add(iterator.next().getKey());
            iterator.remove();
        }
        deletedKeys.forEach( key -> {
            try(Jedis jedis = jedisPool.getResource()) {
                assertNull( jedis.hget(hscanitName, key));
            }
        });
    }


}
