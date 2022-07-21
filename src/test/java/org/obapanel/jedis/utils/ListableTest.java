package org.obapanel.jedis.utils;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ListableTest {

    @Test
    public void asListTest() {
        MyListable myListable = new MyListable(Arrays.asList("a","b","c","a"));
        List<String> result = myListable.asList();
        assertEquals(3, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void asListModifyTest() {
        MyListable myListable = new MyListable(Arrays.asList("a","b","c","a"));
        List<String> result = myListable.asList();
        result.add("d");
    }



    private class MyListable implements Listable<String> {


        private Iterable<String> orign;

        public MyListable(Iterable<String> orign) {
            this.orign = orign;
        }

        @Override
        public List<String> asList() {
            Set<String> data = new HashSet<>();
            orign.forEach(data::add);
            return Collections.unmodifiableList(new ArrayList<>(data));
        }

    }

}
