package org.oba.jedis.extra.utils.streamMessageSystem;

import org.junit.Test;
import redis.clients.jedis.StreamEntryID;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class NextStreamEntryIDTest {

    @Test
    public void nextStreamEntryIDIsNullTest() {
        assertNull(StreamMessageSystem.nextStreamEntryID(null));
    }

    @Test
    public void nextStreamEntryIDIsNotNullTest() {
        long t = System.currentTimeMillis();
        long num = ThreadLocalRandom.current().nextLong(1_000L);
        StreamEntryID currentEntry = new StreamEntryID(t, num);
        assertNotNull(StreamMessageSystem.nextStreamEntryID(currentEntry));
        assertEquals(t, StreamMessageSystem.nextStreamEntryID(currentEntry).getTime());
        assertEquals(num + 1, StreamMessageSystem.nextStreamEntryID(currentEntry).getSequence());
    }

}
