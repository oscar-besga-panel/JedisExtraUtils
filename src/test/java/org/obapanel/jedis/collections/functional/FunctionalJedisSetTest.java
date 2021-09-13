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

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class FunctionalJedisSetTest {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionalJedisListTest.class);

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

    JedisSet createABCSet() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(Arrays.asList("a","b","c"));
        return jedisSet;
    }

    @Test(expected = IllegalStateException.class)
    public void basicTestWithErrorExists() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.checkExists();
    }


    @Test
    public void basicTestExists() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.add("a");
        assertTrue(jedisSet.exists());
    }

    @Test
    public void basicTest() {
        JedisSet jedisSet = createABCSet();
        Set<String> data = jedisSet.asSet();
        assertTrue(jedisSet.exists());
        assertEquals(3, jedisSet.size());
        assertFalse(jedisSet.isEmpty());
        assertEquals(3, data.size());
        assertTrue(data.contains("a"));
        assertTrue(data.contains("b"));
        assertTrue(data.contains("c"));
    }

    @Test
    public void testDataInsertion() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        boolean exists0 = jedisSet.exists();
        boolean add1 = jedisSet.add("a");
        boolean exists1 = jedisSet.exists();
        assertFalse(exists0);
        assertTrue(exists1);
        assertTrue(add1);
        assertTrue(jedisSet.contains("a"));
        assertFalse(jedisSet.contains("b"));
        assertEquals(1, jedisSet.size());
        assertFalse(jedisSet.isEmpty());
        boolean add2 = jedisSet.add("b");
        boolean add3 = jedisSet.add("a");
        assertTrue(add2);
        assertFalse(add3);
        assertTrue(jedisSet.contains("a"));
        assertTrue(jedisSet.contains("b"));
        assertEquals(2, jedisSet.size());
    }

    @Test
    public void testDataMultiInsertionAndContains() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.add("a");
        boolean add1 = jedisSet.addAll(Arrays.asList("b","c"));
        boolean add2 = jedisSet.addAll(Arrays.asList("c","d"));
        boolean add3 = jedisSet.addAll(Arrays.asList("d","a"));
        assertTrue(add1);
        assertTrue(add2);
        assertFalse(add3);
        assertTrue(jedisSet.contains("a"));
        assertTrue(jedisSet.contains("b"));
        assertTrue(jedisSet.containsAll(Arrays.asList("a","b","c","d")));
        assertTrue(jedisSet.containsAll(Arrays.asList("a","d")));
        assertFalse(jedisSet.containsAll(Arrays.asList("a","b","c","e")));
        assertFalse(jedisSet.containsAll(Arrays.asList("a","e","i","o","u")));
    }

    @Test
    public void testDataRemove() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        assertEquals(Integer.valueOf(7), Integer.valueOf(jedisSet.size()));
        assertTrue(jedisSet.remove("b"));
        assertFalse(jedisSet.contains("b"));
        assertEquals(Integer.valueOf(6), Integer.valueOf(jedisSet.size()));
        assertTrue(jedisSet.remove("f"));
        assertFalse(jedisSet.contains("f"));
        assertEquals(Integer.valueOf(5), Integer.valueOf(jedisSet.size()));
        assertFalse(jedisSet.remove("x"));
        assertFalse(jedisSet.contains("x"));
        assertEquals(Integer.valueOf(5), Integer.valueOf(jedisSet.size()));
    }

    @Test
    public void testDataRemoveAll() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        assertEquals(Integer.valueOf(7), Integer.valueOf(jedisSet.size()));
        assertTrue(jedisSet.removeAll(Arrays.asList("b","c")));
        assertEquals(Integer.valueOf(5), Integer.valueOf(jedisSet.size()));
        assertFalse(jedisSet.contains("b"));
        assertTrue(jedisSet.removeAll(Arrays.asList("b","d")));
        assertFalse(jedisSet.contains("d"));
        assertEquals(Integer.valueOf(4), Integer.valueOf(jedisSet.size()));
        assertFalse(jedisSet.removeAll(Arrays.asList("c","b")));
        assertEquals(Integer.valueOf(4), Integer.valueOf(jedisSet.size()));
        assertTrue(jedisSet.removeAll(Arrays.asList("e","f","g")));
        assertFalse(jedisSet.contains("e"));
        assertFalse(jedisSet.contains("f"));
        assertEquals(Integer.valueOf(1), Integer.valueOf(jedisSet.size()));
    }

    @Test
    public void testDataRetainAll() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        boolean result1 = jedisSet.retainAll(Arrays.asList("a", "b", "c", "x", "y"));
        assertEquals(Integer.valueOf(3), Integer.valueOf(jedisSet.size()));
        assertTrue(jedisSet.contains("a"));
        assertTrue(jedisSet.contains("b"));
        assertTrue(jedisSet.contains("c"));
        assertTrue(result1);
        boolean result2 = jedisSet.retainAll(Arrays.asList("a", "b", "c", "x", "y"));
        assertFalse(result2);
    }


}
