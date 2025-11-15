package org.oba.jedis.extra.utils.collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class })
public class JedisSetStreamTest {

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

    private static final List<String> initialData = Collections.unmodifiableList(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));

    JedisSet createABCDEFGSet() {
        JedisSet jedisSet = new JedisSet(mockOfJedisForSet.getJedisPool(), setName);
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
