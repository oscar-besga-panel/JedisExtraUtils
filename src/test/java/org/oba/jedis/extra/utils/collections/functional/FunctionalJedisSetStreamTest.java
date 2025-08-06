package org.oba.jedis.extra.utils.collections.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.collections.JedisSet;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.oba.jedis.extra.utils.test.WithJedisPoolDelete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class FunctionalJedisSetStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisSetStreamTest.class);

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
            WithJedisPoolDelete.doDelete(jedisPool, setName);
            jedisPool.close();
        }
    }

    private static final List<String> initialData = Collections.unmodifiableList(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

    JedisSet createABCDEFGSet() {
        JedisSet jedisSet = new JedisSet(jedisPool, setName);
        jedisSet.addAll(initialData);
        return jedisSet;
    }

    @Test
    public void forEachTest() {
        StringBuilder sb = new StringBuilder();
        createABCDEFGSet().forEach(sb::append);
        initialData.forEach( s -> {
            assertTrue(sb.toString().contains(s));
        });
    }

    @Test
    public void streamForEachTest() {
        StringBuilder sb = new StringBuilder();
        createABCDEFGSet().stream().
                forEach(sb::append);
        initialData.forEach( s -> {
            assertTrue(sb.toString().contains(s));
        });
    }

    @Test
    public void streamComplex1Test() {
        StringBuilder sb = new StringBuilder();
        createABCDEFGSet().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                forEach(sb::append);
        initialData.forEach( s -> {
            if (!s.contains("b")) {
                assertTrue(sb.toString().contains(s + ","));
            }
        });
    }

    @Test
    public void streamComplex2Test() {
        StringBuilder sb = new StringBuilder();
        boolean result = createABCDEFGSet().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                peek(sb::append).
                anyMatch( s -> s.length() > 1);
        AtomicBoolean anyExcepB = new AtomicBoolean(false);
        initialData.forEach( s -> {
            if (!s.contains("b")) {
                boolean b = sb.toString().contains(s + ",");
                anyExcepB.set(anyExcepB.get() || b );
            }

        });
        assertTrue(anyExcepB.get());
        assertTrue(result);
    }


    @Test
    public void streamComplex3Test() {
        StringBuilder sb = new StringBuilder();
        boolean result = createABCDEFGSet().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                peek(sb::append).
                allMatch( s -> s.length() > 1);
        initialData.forEach( s -> {
            if (!s.contains("b")) {
                assertTrue(sb.toString().contains(s + ","));
            }
        });
        assertTrue(result);
    }

    @Test
    public void streamComplex4Test() {
        boolean result = createABCDEFGSet().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                findAny().
                isPresent();
        assertTrue(result);
    }

    @Test
    public void streamComplex5Test() {
        StringBuilder sb = new StringBuilder();
        boolean result = createABCDEFGSet().stream().
                filter(s -> !s.equalsIgnoreCase("B")).
                map( s -> s + ",").
                peek(sb::append).
                findFirst().
                isPresent();
        AtomicBoolean anyExcepB = new AtomicBoolean(false);
        initialData.forEach( s -> {
            if (!s.contains("b")) {
                boolean b = sb.toString().contains(s + ",");
                anyExcepB.set(anyExcepB.get() || b );
            }

        });
        assertTrue(anyExcepB.get());
        assertTrue(result);
    }

}
