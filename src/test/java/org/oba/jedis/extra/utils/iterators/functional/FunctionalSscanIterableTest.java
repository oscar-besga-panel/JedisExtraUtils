package org.oba.jedis.extra.utils.iterators.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.iterators.SScanIterable;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FunctionalSscanIterableTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalSscanIterableTest.class);



    private static AtomicInteger count = new AtomicInteger(0);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String sscanitName;
    private List<String> letters;
    private JedisPooled jedisPooled;


    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        sscanitName = "scanIterable:" + this.getClass().getName() + ":" + System.currentTimeMillis() + ":" + count.incrementAndGet();
        jedisPooled = jtfTest.createJedisPooled();
        letters = jtfTest.randomSizedListOfChars();
        LOGGER.debug("before count {} for name {} with letters {}", count.get(), sscanitName, letters );
    }

    @After
    public void after() {
        if (jedisPooled != null) {
            jedisPooled.del(sscanitName);
            jedisPooled.close();
        }
    }

    void createABCData() {
        letters.forEach( letter -> {
            jedisPooled.sadd(sscanitName, letter);
        });
    }

    @Test
    public void iteratorEmptyTest() {
        int num = 0;
        SScanIterable sscanIterable = new SScanIterable(jedisPooled,sscanitName,  "*");
        Iterator<String> iterator =  sscanIterable.iterator();
        StringBuilder sb = new StringBuilder();
        while(iterator.hasNext()) {
            num++;
            sb.append(iterator.next());
        }
        assertNotNull(iterator);
        assertTrue(sb.length() == 0);
        assertTrue(num == 0);
        assertEquals(sscanitName, sscanIterable.getName());
    }

    @Test
    public void iteratorEmpty2Test() {
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, 20);
        List<String> data = sscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorEmpty3Test() {
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, 20);
        List<String> data = sscanIterable.asList();
        assertTrue(data.isEmpty());
    }

    @Test
    public void iteratorWithResultsTest() {
        int num = 0;
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(jedisPooled,sscanitName,  "*");
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
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, "*");
        Iterator<String> iterator =  sscanIterable.iterator();
        while(iterator.hasNext()) {
            assertTrue( jedisPooled.exists(sscanitName));
            assertTrue( jedisPooled.sismember(sscanitName, iterator.next()));
        }
    }

    @Test
    public void iteratorWithResultsForEachTest() {
        AtomicInteger num = new AtomicInteger(0);
        StringBuilder sb = new StringBuilder();
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, "*");
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
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, "*");
        sscanIterable.forEach( key -> {
            assertTrue( jedisPooled.exists(sscanitName));
            assertTrue( jedisPooled.sismember(sscanitName, key));
        });
    }


    @Test
    public void iteratorRemoveForEach1Test() {
        createABCData();
        jedisPooled.sadd(sscanitName, "extra");
        List<String> deleted = new ArrayList<>();
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, "*");
        Iterator<String> iterator = sscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next());
            iterator.remove();
        }
        deleted.forEach( key -> {
            assertFalse( jedisPooled.sismember(sscanitName, key));
        });
    }

    @Test
    public void iteratorRemoveForEach2Test() {
        createABCData();
        List<String> deleted = new ArrayList<>();
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, "*");
        Iterator<String> iterator = sscanIterable.iterator();
        while (iterator.hasNext()) {
            deleted.add( iterator.next());
            iterator.remove();
        }
        deleted.forEach( key -> {
            assertFalse( jedisPooled.sismember(sscanitName, key));
        });
    }

    @Test
    public void asListTest() {
        createABCData();
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName);
        List<String> data = sscanIterable.asList();
        letters.forEach( letter -> {
            assertTrue(data.contains(letter));
        });
        assertEquals(letters.size(), data.size());
    }

    @Test(expected = IllegalStateException.class)
    public void errorInDeleteTest() {
        SScanIterable sscanIterable = new SScanIterable(jedisPooled, sscanitName, 20);
        Iterator<String> iterator = sscanIterable.iterator();
        iterator.remove();
    }

}
