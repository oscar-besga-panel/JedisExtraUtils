package org.oba.jedis.extra.utils.collections.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.oba.jedis.extra.utils.collections.JedisList;
import org.oba.jedis.extra.utils.test.JedisTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.oba.jedis.extra.utils.utils.functional.WithJedisPoolDelete.doDelete;

public class FunctionalJedisListIteratorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionalJedisListIteratorTest.class);

    private static final AtomicInteger testNumber = new AtomicInteger(0);

    private final JedisTestFactory jtfTest = JedisTestFactory.get();

    private String listName;
    private JedisPool jedisPool;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(jtfTest.functionalTestEnabled());
        if (!jtfTest.functionalTestEnabled()) return;
        listName = "list:" + this.getClass().getName() + ":" + testNumber.incrementAndGet() + "_" + System.currentTimeMillis();
        jedisPool = jtfTest.createJedisPool();
    }

    @After
    public void after() {
        if (jedisPool != null) {
            doDelete(jedisPool, listName);
            jedisPool.close();
        }
    }



    private JedisList createABCList(){
        JedisList jedisList = new JedisList(jedisPool, listName);
        jedisList.addAll(Arrays.asList("a", "b", "c"));
        return jedisList;
    }

    @Test
    public void listIteratorBasicTest() {
        JedisList jedisList = createABCList();
        try {
            jedisList.listIterator(5);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // NOOP
        }
        ListIterator<String> it = jedisList.listIterator(1);
        assertTrue(it.hasNext());
        assertEquals("b", it.next());
    }

    @Test
    public void listIteratorAdvancedTest() {
        JedisList jedisList = createABCList();
        ListIterator<String> listIterator = jedisList.listIterator();
        assertTrue(listIterator.hasNext());
        assertEquals("a", listIterator.next());
        assertEquals(2, listIterator.nextIndex());
        assertEquals(0, listIterator.previousIndex());
        assertTrue(listIterator.hasNext());
        assertEquals("b", listIterator.next());
        assertEquals(3, listIterator.nextIndex());
        assertEquals(1, listIterator.previousIndex());
        assertTrue(listIterator.hasPrevious());
        assertEquals("b", listIterator.previous());
        assertEquals(2, listIterator.nextIndex());
        assertEquals(0, listIterator.previousIndex());
        assertTrue(listIterator.hasPrevious());
        assertEquals("a", listIterator.previous());
        assertEquals(1, listIterator.nextIndex());
        assertEquals(-1, listIterator.previousIndex());
        assertFalse(listIterator.hasPrevious());
        listIterator.set("A");
        assertTrue(listIterator.hasNext());
        assertEquals("A", listIterator.next());
        assertEquals(2, listIterator.nextIndex());
        assertEquals(0, listIterator.previousIndex());
        listIterator.add("B");
        assertTrue(listIterator.hasNext());
        assertEquals("B", listIterator.next());
        assertEquals(3, listIterator.nextIndex());
        assertEquals(1, listIterator.previousIndex());

        assertTrue(listIterator.hasNext());
        assertEquals("b", listIterator.next());
        assertEquals(4, listIterator.nextIndex());
        assertEquals(2, listIterator.previousIndex());
        listIterator.remove();
        assertTrue(listIterator.hasPrevious());
        assertEquals("B", listIterator.previous());
    }


    @Test
    public void listIteratorAdvancedTest2() {
        List<String> data = createABCList().asList();
        ListIterator<String> listIterator = data.listIterator();
        assertTrue(listIterator.hasNext());
        assertEquals("a", listIterator.next());
        assertTrue(listIterator.hasNext());
        assertEquals("b", listIterator.next());
        assertTrue(listIterator.hasPrevious());
        assertEquals("b", listIterator.previous());
        assertTrue(listIterator.hasPrevious());
        assertEquals("a", listIterator.previous());
        assertFalse(listIterator.hasPrevious());
    }

    @Test
    public void listIteratorBasicWhileTest1() {
        List<String> check = new ArrayList<>();
        JedisList jedisList = createABCList();
        Iterator<String> it = jedisList.iterator();
        while(it.hasNext()) {
            String s = it.next();
            assertTrue(jedisList.contains(s));
            check.add(s);
        }
        assertTrue( check.size() == jedisList.size() );
        assertTrue( jedisList.containsAll(check) );
        assertTrue( check.containsAll(jedisList) );
    }

    @Test
    public void listIteratorBasicWhileTest2() {
        List<String> check = new ArrayList<>();
        JedisList jedisList = new JedisList(jedisPool, listName);
        jedisList.add("a");
        Iterator<String> it = jedisList.iterator();
        while(it.hasNext()) {
            String s = it.next();
            assertTrue(jedisList.contains(s));
            check.add(s);
        }
        assertTrue( check.size() == jedisList.size() );
        assertTrue( jedisList.containsAll(check) );
        assertTrue( check.containsAll(jedisList) );
    }


    @Test
    public void listIteratorBasicWhileTest3() {
        List<String> check = new ArrayList<>();
        JedisList jedisList = new JedisList(jedisPool, listName);
        Iterator<String> it = jedisList.iterator();
        while(it.hasNext()) {
            String s = it.next();
            assertTrue(jedisList.contains(s));
            check.add(s);
        }
        assertTrue( check.size() == jedisList.size() );
        assertTrue( jedisList.containsAll(check) );
        assertTrue( check.containsAll(jedisList) );
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void listIteratorRemoveOneTestError() {
        JedisList jedisList = new JedisList(jedisPool, listName);
        jedisList.add("a");
        Iterator<String> it = jedisList.iterator();
        it.remove();
    }

    @Test
    public void listIteratorRemoveOneTest() {
        JedisList jedisList = new JedisList(jedisPool, listName);
        jedisList.add("a");
        Iterator<String> it = jedisList.iterator();
        if (it.hasNext()) {
            it.next();
            it.remove();
        }
        assertFalse(it.hasNext());
    }

}
