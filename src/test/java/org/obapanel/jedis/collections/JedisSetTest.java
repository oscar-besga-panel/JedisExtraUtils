package org.obapanel.jedis.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.obapanel.jedis.collections.MockOfJedisForList.unitTestEnabledForList;

public class JedisSetTest {

    private String setName;
    private MockOfJedisForSet mockOfJedisForSet;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabledForList());
        if (!unitTestEnabledForList()) return;
        setName = "set:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedisForSet = new MockOfJedisForSet();
    }

    @After
    public void after() {
        if (mockOfJedisForSet != null) mockOfJedisForSet.clearData();
    }

    JedisSet createABCSet() {
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
        jedisSet.addAll(Arrays.asList("a","b","c"));
        return jedisSet;
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
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
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
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
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


}
