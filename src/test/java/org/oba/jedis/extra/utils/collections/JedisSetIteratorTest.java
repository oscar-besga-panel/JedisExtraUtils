package org.oba.jedis.extra.utils.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class JedisSetIteratorTest {


    private String setName;
    private MockOfJedisForSet mockOfJedisForSet;

    @Before
    public void before() {
        setName = "set:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedisForSet = new MockOfJedisForSet();
    }

    @After
    public void after() {
        if (mockOfJedisForSet != null) mockOfJedisForSet.clearData();
    }

    @Test
    public void testIterator() {
        List<String> data = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
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
        List<String> data = new ArrayList<>(Collections.singletonList("a"));
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
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
        List<String> data = new ArrayList<>();
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
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
        List<String> todel = new ArrayList<>(Arrays.asList("a", "d", "g"));
        List<String> data = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
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
