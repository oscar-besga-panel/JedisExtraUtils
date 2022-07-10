package org.obapanel.jedis.cache.javaxcache;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RedisCacheEntryTest {



    @Test
    public void entryTest(){
        RedisCacheEntry entry = new RedisCacheEntry("a", "A1");
        assertEquals("a", entry.getKey());
        assertEquals("A1", entry.getValue());
        assertTrue(entry.toString().contains("a"));
        assertTrue(entry.toString().contains("A1"));
        RedisCacheEntry entry2 = new RedisCacheEntry("a", "A1");
        assertEquals(entry2, entry);
        assertEquals(entry2.hashCode(), entry.hashCode());
        assertTrue(entry.equals(entry2));
        RedisCacheEntry entry3 = new RedisCacheEntry("b", "B1");
        assertNotEquals(entry3, entry);
        assertNotEquals(entry3.hashCode(), entry.hashCode());
        assertFalse(entry.equals(entry3));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void entryUnsupportedTest(){
        RedisCacheEntry entry = new RedisCacheEntry("a", "A1");
        entry.setValue("A2");
    }

}
