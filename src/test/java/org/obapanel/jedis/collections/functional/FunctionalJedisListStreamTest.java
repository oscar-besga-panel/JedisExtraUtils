package org.obapanel.jedis.collections.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.obapanel.jedis.collections.JedisList;
import org.obapanel.jedis.common.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FunctionalJedisListStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisListIteratorTest.class);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String listName;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        listName = "list:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }


    private JedisList createABCDEList(){
        JedisList jedisList = new JedisList(jedisPool, listName);
        jedisList.addAll(Arrays.asList("a", "b", "c", "d", "e"));
        return jedisList;
    }

    @Test
    public void forEachTest() {
        StringBuilder sb = new StringBuilder();
        createABCDEList().forEach(sb::append);
        assertEquals("abcde", sb.toString());
    }

    @Test
    public void streamForEachTest() {
        StringBuilder sb = new StringBuilder();
        createABCDEList().stream().
                forEach(sb::append);
        assertEquals("abcde", sb.toString());
    }

    @Test
    public void streamComplex1Test() {
        StringBuilder sb = new StringBuilder();
        createABCDEList().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                forEach(sb::append);
        assertEquals("a,c,d,e,", sb.toString());
    }

    @Test
    public void streamComplex2Test() {
        StringBuilder sb = new StringBuilder();
        boolean result = createABCDEList().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                peek(sb::append).
                anyMatch( s -> s.length() > 1);
        assertEquals("a,", sb.toString());
        assertTrue(result);
    }


    @Test
    public void streamComplex3Test() {
        StringBuilder sb = new StringBuilder();
        boolean result = createABCDEList().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                peek(sb::append).
                allMatch( s -> s.length() > 1);
        assertEquals("a,c,d,e,", sb.toString());
        assertTrue(result);
    }

    @Test
    public void streamComplex4Test() {
        boolean result = createABCDEList().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                findAny().
                isPresent();
        assertTrue(result);
    }

    @Test
    public void streamComplex5Test() {
        StringBuilder sb = new StringBuilder();
        boolean result = createABCDEList().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                peek(sb::append).
                findFirst().
                isPresent();
        assertEquals("a,", sb.toString());
        assertTrue(result);
    }

}
