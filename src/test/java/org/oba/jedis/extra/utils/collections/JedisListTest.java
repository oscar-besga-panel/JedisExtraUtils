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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(PowerMockRunner.class)
@PrepareForTest({Transaction.class, TransactionBase.class })
public class JedisListTest {


    private String listName;
    private MockOfJedisForList mockOfJedisForList;

    @Before
    public void before() {
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
    public void getNameTest() {
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
        assertEquals(listName, jedisList.getName());
    }

    @Test(expected = IllegalStateException.class)
    public void basicTestWithErrorExists() {
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
        jedisList.checkExists();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void basicTestWithErrorIndex() {
        JedisList jedisList = createABCList();
        jedisList.checkIndex(4);
    }

    @Test
    public void basicTestClear() {
        JedisList jedisList = createABCList();
        jedisList.clear();
        assertFalse(jedisList.exists());
        assertTrue(jedisList.isEmpty());
    }

    @Test
    public void basicTest() {
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
        assertFalse(jedisList.exists());
        assertTrue(jedisList.isEmpty());
        assertEquals(0L, jedisList.size());
        jedisList.add("a");
        assertTrue(jedisList.exists());
        assertFalse(jedisList.isEmpty());
        jedisList.checkExists();
        assertEquals(1L, jedisList.size());
        jedisList.addAll(Arrays.asList("b", "c"));
        assertEquals(3L, jedisList.size());
        List<String> tmp = jedisList.asList();
        assertTrue(tmp.containsAll(Arrays.asList("a", "b", "c")));
        assertTrue(Arrays.asList("a", "b", "c").containsAll(tmp));
        int num = 0;
        Iterator<String> it = jedisList.iterator();
        while(it.hasNext()){
            num++;
            assertTrue(Arrays.asList("a", "b", "c").contains(it.next()));
        }
        assertEquals(num, jedisList.size());
        num = 0;
        for(String s: jedisList) {
            num++;
            assertTrue(Arrays.asList("a", "b", "c").contains(s));
        }
        assertEquals(num, jedisList.size());
        ArrayList tmp2 = new ArrayList<>(jedisList);
        assertTrue(Arrays.asList("a", "b", "c").containsAll(tmp2));
        assertTrue(tmp2.containsAll(Arrays.asList("a", "b", "c")));
        assertEquals("a", jedisList.toArray()[0]);
        assertEquals("b", jedisList.toArray(new String[]{})[1]);
        assertEquals("c", jedisList.toArray(new String[jedisList.size()])[2]);
    }


    @Test
    public void removeByIndexTest() {
        JedisList jedisList = createABCList();
        assertEquals(3L, jedisList.size());
        String removed = jedisList.remove(1);
        assertEquals(2L, jedisList.size());
        assertEquals("b", removed);
    }

    @Test
    public void removeByDataTest() {
        JedisList jedisList = createABCList();
        assertEquals(3L, jedisList.size());
        boolean removed = jedisList.remove("b");
        assertEquals(2L, jedisList.size());
        assertTrue(removed);
        assertTrue(Arrays.asList("a", "c").containsAll(jedisList));
        assertTrue(jedisList.containsAll(Arrays.asList("a", "c")));
    }


    @Test
    public void setAddTest() {
        JedisList jedisList = createABCList();
        assertEquals(3L, jedisList.size());
        String removed = jedisList.set(1,"B");
        assertEquals(3L, jedisList.size());
        assertEquals("b", removed);
        assertEquals("B", jedisList.get(1));
        jedisList.add(1,"b");
        assertEquals(4L, jedisList.size());
        assertEquals("a", jedisList.get(0));
        assertEquals("b", jedisList.get(1));
        assertEquals("B", jedisList.get(2));
        assertEquals("c", jedisList.get(3));
    }

    @Test
    public void containsIndexOf(){
        JedisList jedisList = new JedisList(mockOfJedisForList.getJedisPool(), listName);
        jedisList.addAll(Arrays.asList("a", "b", "c", "a", "d"));
        assertTrue(jedisList.contains("a"));
        assertEquals(0, jedisList.indexOf("a"));
        assertEquals(3, jedisList.lastIndexOf("a"));
        assertTrue(jedisList.contains("c"));
        assertEquals(2, jedisList.indexOf("c"));
        assertEquals(2, jedisList.lastIndexOf("c"));
        assertFalse(jedisList.contains("C"));
        assertEquals(-1, jedisList.indexOf("C"));
        assertEquals(-1, jedisList.lastIndexOf("C"));
    }

    @Test
    public void addAllTest() {
        JedisList jedisList = createABCList();
        jedisList.addAll(1, Arrays.asList("A", "B"));
        List fromRedis = jedisList.asList();
        List example = new ArrayList();
        example.add("a");
        example.add("b");
        example.add("c");
        example.addAll(1, Arrays.asList("A", "B"));
        fromRedis.containsAll(example);
        example.containsAll(fromRedis);
        for(int i= 0; i < fromRedis.size(); i++){
            assertEquals(example.get(i), fromRedis.get(i));
        }
    }

    @Test
    public void removeAllTest() {
        JedisList jedisList = createABCList();
        jedisList.removeAll( Arrays.asList("a", "c"));
        assertEquals(1, jedisList.size());
        assertEquals("b", jedisList.get(0));
    }

    @Test
    public void retainAllTest() {
        JedisList jedisList = createABCList();
        jedisList.retainAll( Arrays.asList("a", "c"));
        assertEquals(2, jedisList.size());
        assertEquals("a", jedisList.get(0));
        assertEquals("c", jedisList.get(1));
        assertEquals(-1,  jedisList.indexOf("b"));
    }

    @Test
    public void subListTest() {
        JedisList jedisList = createABCList();
        List<String> subList1 = jedisList.subList(1,3);
        assertEquals(2, subList1.size());
        assertEquals("b", subList1.get(0));
        assertEquals("c", subList1.get(1));
        List<String> subList2 = jedisList.subList(0, 1);
        assertEquals(1, subList2.size());
        assertEquals("a", subList2.get(0));
        String listName2 = listName + "_2";
        JedisList jedisList3 = jedisList.jedisSubList(listName2, 1,3);
        assertTrue(jedisList3.exists());
        assertEquals(2, jedisList3.size());
        assertEquals("b", jedisList3.get(0));
        assertEquals("c", jedisList3.get(1));
        JedisList jedisList4 = new JedisList(mockOfJedisForList.getJedisPool(), listName2);
        assertTrue(jedisList4.exists());
        assertEquals(2, jedisList4.size());
        assertEquals("b", jedisList4.get(0));
        assertEquals("c", jedisList4.get(1));
        mockOfJedisForList.getJedis().del(listName2);
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


}
