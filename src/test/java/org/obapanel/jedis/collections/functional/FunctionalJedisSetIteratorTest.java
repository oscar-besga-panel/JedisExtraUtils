package org.obapanel.jedis.collections.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.collections.JedisSet;
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
import static org.obapanel.jedis.collections.functional.JedisTestFactory.createJedisPool;
import static org.obapanel.jedis.collections.functional.JedisTestFactory.functionalTestEnabled;

public class FunctionalJedisSetIteratorTest {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionalJedisSetIteratorTest.class);

    private String setName;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        setName = "set:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = createJedisPool();
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
        LOG.debug("TEST ITERATOR --\n-");
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
        LOG.debug("TEST ITERATOR --\n-");
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
        LOG.debug("TEST ITERATOR --\n-");
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
        LOG.debug("TEST ITERATOR --\n-");
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
