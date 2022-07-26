package org.obapanel.jedis.collections;

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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.obapanel.jedis.collections.MockOfJedisForList.unitTestEnabledForList;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class JedisListIteratorTest {


    private String listName;
    private MockOfJedisForList mockOfJedisForList;

    @Before
    public void before() {
        org.junit.Assume.assumeTrue(unitTestEnabledForList());
        if (!unitTestEnabledForList()) return;
        listName = "list:" + this.getClass().getName() + ":" + System.currentTimeMillis();
        mockOfJedisForList = new MockOfJedisForList();
    }

    @After
    public void after() {
        if (mockOfJedisForList != null) mockOfJedisForList.clearData();
    }



    private JedisList createABCList(){
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
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
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
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
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
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
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
        jedisList.add("a");
        Iterator<String> it = jedisList.iterator();
        it.remove();
    }

    @Test
    public void listIteratorRemoveOneTest() {
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
        jedisList.add("a");
        Iterator<String> it = jedisList.iterator();
        if (it.hasNext()) {
            it.next();
            it.remove();
        }
        assertFalse(it.hasNext());
    }

}
