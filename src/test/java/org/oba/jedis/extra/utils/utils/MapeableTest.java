package org.oba.jedis.extra.utils.utils;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MapeableTest {

    @Test
    public void asListTest() {
        MyMapeable myMapeable = new MyMapeable(createData());
        Map<String, String> result = myMapeable.asMap();
        assertEquals(3, result.size());
        assertTrue(result.containsKey("a"));
        assertTrue(result.containsKey("b"));
        assertTrue(result.containsKey("c"));
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
        assertEquals("3", result.get("c"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void asMapModifyTest() {
        MyMapeable myMapeable = new MyMapeable(createData());
        myMapeable.asMap().put("z","99");
    }

    private Iterable<Map.Entry<String, String>> createData() {
        Map<String,String> data = new HashMap<>();
        data.put("a","1");
        data.put("b","2");
        data.put("c","3");
        return data.entrySet();
    }

    private static class MyMapeable implements Mapeable<String, String> {

        private Iterable<Map.Entry<String, String>> origin;

        public MyMapeable(Iterable<Map.Entry<String, String>> origin) {
            this.origin = origin;
        }

        @Override
        public Map<String, String> asMap() {
            Map<String, String> map = new HashMap<>();
            origin.forEach( e -> map.put(e.getKey(), e.getValue()));
            return Collections.unmodifiableMap(map);
        }
    }
}
