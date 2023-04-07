package org.oba.jedis.extra.utils.collections;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBase;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class JedisListStreamTest {

    private String listName;
    private MockOfJedisForList mockOfJedisForList;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(MockOfJedisForList.unitTestEnabledForList());
        if (!MockOfJedisForList.unitTestEnabledForList()) return;
        listName = "list:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedisForList = new MockOfJedisForList();
    }

    @After
    public void after() {
        if (mockOfJedisForList != null) mockOfJedisForList.clearData();
    }


    private JedisList createABCDEList(){
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
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
