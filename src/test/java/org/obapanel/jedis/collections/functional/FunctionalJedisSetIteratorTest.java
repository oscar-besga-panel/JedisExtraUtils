package org.obapanel.jedis.collections.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.collections.JedisSet;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FunctionalJedisSetIteratorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisSetIteratorTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String setName;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        setName = "set:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) {
            try(Jedis jedis = jedisPool.getResource()) {
                jedis.del(setName);
            }
            jedisPool.close();
        }
    }

    @Test
    public void testIterator() {
        LOGGER.debug("TEST ITERATOR --\n-");
        List<String> data = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(data);
        Iterator<String> iterator = jedisSet.iterator();
        while (iterator.hasNext()) {
            String s = iterator.next();
            assertTrue(data.contains(s));
            assertTrue(data.remove(s));
        }
        assertTrue(data.isEmpty());
        assertEquals(Integer.valueOf(7), Integer.valueOf(jedisSet.size()));
        assertTrue(jedisSet.containsAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g")));
    }

    @Test
    public void testIteratorOne() {
        LOGGER.debug("TEST ITERATOR --\n-");
        List<String> data = new ArrayList<>(Collections.singletonList("a"));
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(data);
        Iterator<String> iterator = jedisSet.iterator();
        while (iterator.hasNext()) {
            String s = iterator.next();
            assertTrue(data.contains(s));
            assertTrue(data.remove(s));
        }
        assertTrue(data.isEmpty());
        assertEquals(Integer.valueOf(1), Integer.valueOf(jedisSet.size()));
        assertTrue(jedisSet.contains("a"));
    }

    @Test
    public void testIteratorNone() {
        LOGGER.debug("TEST ITERATOR --\n-");
        List<String> data = new ArrayList<>();
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(data);
        Iterator<String> iterator = jedisSet.iterator();
        while (iterator.hasNext()) {
            String s = iterator.next();
            assertTrue(data.contains(s));
            assertTrue(data.remove(s));
        }
        assertTrue(data.isEmpty());
        assertEquals(Integer.valueOf(0), Integer.valueOf(jedisSet.size()));
    }

    @Test
    public void testIteratorRemove() {
        LOGGER.debug("TEST ITERATOR --\n-");
        List<String> todel = new ArrayList<>(Arrays.asList("a", "d", "g"));
        List<String> data = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(data);
        Iterator<String> iterator = jedisSet.iterator();
        while (iterator.hasNext()) {
            String s = iterator.next();
            assertTrue(data.contains(s));
            assertTrue(data.remove(s));
            if (todel.contains(s)) {
                iterator.remove();
            }
        }
        assertTrue(data.isEmpty());
        assertEquals(Integer.valueOf(4), Integer.valueOf(jedisSet.size()));
        assertFalse(jedisSet.containsAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g")));
        assertTrue(jedisSet.containsAll(Arrays.asList("b", "c", "e", "f")));
    }

}
