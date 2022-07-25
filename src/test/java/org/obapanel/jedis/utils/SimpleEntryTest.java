package org.obapanel.jedis.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class SimpleEntryTest {

    @Test
    public void simpleEntryTest() {
        SimpleEntry simpleEntry1a = new SimpleEntry("a","A1");
        SimpleEntry simpleEntry1b = new SimpleEntry("a","A1");
        SimpleEntry simpleEntry2a = new SimpleEntry("b","B1");
        assertTrue(simpleEntry1a.equals(simpleEntry1b));
        assertFalse(simpleEntry1a.equals(simpleEntry2a));
        assertEquals(simpleEntry1a.hashCode(), simpleEntry1b.hashCode());
        assertNotEquals(simpleEntry1a.hashCode(), simpleEntry2a.hashCode());
    }

    @Test
    public void simpleEntryToStringTest() {
        SimpleEntry simpleEntry1a = new SimpleEntry("ax","AX1");
        assertTrue(simpleEntry1a.toString().contains("AX1"));
        assertTrue(simpleEntry1a.toString().contains("ax"));
    }

    @Test
    public void simpleEntryGetterTest() {
        SimpleEntry simpleEntry1a = new SimpleEntry("ax","AX1");
        assertEquals("ax", simpleEntry1a.getKey());
        assertEquals("AX1", simpleEntry1a.getValue());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void simpleEntrySetterTest() {
        SimpleEntry simpleEntry1a = new SimpleEntry("ax","AX1");
        simpleEntry1a.setValue("AX2");
    }

}

