package org.oba.jedis.extra.utils.notificationLock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class MockOfJedisTest {

    private MockOfJedis mockOfJedis;

    private ExecutorService executorService;

    @Before
    public void setup() {
        org.junit.Assume.assumeTrue(MockOfJedis.unitTestEnabled());
        if (!MockOfJedis.unitTestEnabled()) return;
        mockOfJedis = new MockOfJedis();
        executorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        if (mockOfJedis != null) {
            mockOfJedis.clearData();
        }
        executorService.shutdown();
    }

    @Test
    public void existsTest() {
        assertNotNull(mockOfJedis.getJedis());
    }

    @Test
    public void xadd1Test() {
        String t = Long.toString(System.currentTimeMillis());
        StreamEntryID streamEntryID1 = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("hola", "caracola", "t", t));
        assertFalse(mockOfJedis.getData().isEmpty());
        assertFalse(mockOfJedis.getData().get("stream1").isEmpty());
        assertFalse(mockOfJedis.getData().get("stream1").get(streamEntryID1).isEmpty());
        assertEquals(t, mockOfJedis.getData().get("stream1").get(streamEntryID1).get("t"));
        assertNull(mockOfJedis.getData().get("stream2"));
    }

    @Test
    public void xadd2Test() {
        String t = Long.toString(System.currentTimeMillis());
        StreamEntryID streamEntryID1 = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("hola", "caracola", "t", t, "n", "0"));
        StreamEntryID streamEntryID2 = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("hola", "caracola", "t", t, "n", "1"));
        StreamEntryID streamEntryID3 = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("hola", "caracola", "t", t, "n", "2"));
        assertFalse(mockOfJedis.getData().isEmpty());
        assertFalse(mockOfJedis.getData().get("stream1").isEmpty());
        assertFalse(mockOfJedis.getData().get("stream1").get(streamEntryID1).isEmpty());
        assertFalse(mockOfJedis.getData().get("stream1").get(streamEntryID2).isEmpty());
        assertFalse(mockOfJedis.getData().get("stream1").get(streamEntryID3).isEmpty());
        assertEquals("0", mockOfJedis.getData().get("stream1").get(streamEntryID1).get("n"));
        assertEquals("1", mockOfJedis.getData().get("stream1").get(streamEntryID2).get("n"));
        assertEquals("2", mockOfJedis.getData().get("stream1").get(streamEntryID3).get("n"));
        assertTrue((streamEntryID3.getTime() > streamEntryID2.getTime()) ||
                ((streamEntryID3.getTime() == streamEntryID2.getTime()) && (streamEntryID3.getSequence() > streamEntryID2.getSequence())));
        assertTrue((streamEntryID2.getTime() > streamEntryID1.getTime()) ||
                ((streamEntryID2.getTime() == streamEntryID1.getTime()) && (streamEntryID2.getSequence() > streamEntryID1.getSequence())));
    }

    @Test
    public void convertToStreamEntryTest() {
        long t = System.currentTimeMillis();
        Map<StreamEntryID, Map<String, String>> data = new HashMap<>();
        data.put(new StreamEntryID(t, 0), Map.of("message", "message234"));
        StreamEntry entry = mockOfJedis.convertToStreamEntry(data.entrySet().iterator().next());
        assertEquals(t, entry.getID().getTime());
        assertEquals(0, entry.getID().getSequence());
        assertEquals("message234", entry.getFields().get("message"));
    }

    @Test
    public void isEntryBetweenTest() {
        long t = System.currentTimeMillis();
        StreamEntryID seid1 = new StreamEntryID(t, 0);
        StreamEntryID seid2 = new StreamEntryID(t, 1);
        StreamEntryID seid3 = new StreamEntryID(t, 2);
        StreamEntryID seid4 = new StreamEntryID(t + 1, 0);
        assertTrue(mockOfJedis.isEntryBetween(seid2, seid1, seid3));
        assertFalse(mockOfJedis.isEntryBetween(seid1, seid2, seid3));
        assertFalse(mockOfJedis.isEntryBetween(seid3, seid1, seid2));
        assertTrue(mockOfJedis.isEntryBetween(seid2, seid1, seid4));
        assertTrue(mockOfJedis.isEntryBetween(seid3, seid2, seid4));
        assertFalse(mockOfJedis.isEntryBetween(seid4, seid1, seid3));
        assertFalse(mockOfJedis.isEntryBetween(seid1, seid2, seid4));
    }

    @Test
    public void xrange1Test() {
        StreamEntryID entryId = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("message", "message456"));
        StreamEntryID before = new StreamEntryID(entryId.getTime() - 1, 0);
        StreamEntryID after = new StreamEntryID(entryId.getTime() + 1, 0);
        List<StreamEntry> entries = mockOfJedis.xrange("stream1", before, after);
        assertFalse(entries.isEmpty());
        assertEquals(entryId, entries.get(0).getID());
        assertEquals("message456", entries.get(0).getFields().get("message"));
    }

    @Test
    public void xrange2Test() throws InterruptedException {
        StreamEntryID entryId1 = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("message", "message1"));
        Thread.sleep(50);
        StreamEntryID entryId2 = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("message", "message2"));
        Thread.sleep(50);
        StreamEntryID entryId3 = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("message", "message3"));

        StreamEntryID e01 = new StreamEntryID(entryId1.getTime() - 1, 0);
        StreamEntryID e12 = new StreamEntryID(entryId1.getTime() + 1, 0);
        StreamEntryID e23 = new StreamEntryID(entryId2.getTime() + 1, 0);
        StreamEntryID e3N = new StreamEntryID(entryId3.getTime() + 1, 0);

        List<StreamEntry> entries1 = mockOfJedis.xrange("stream1", e01, e3N);
        assertFalse(entries1.isEmpty());
        assertTrue(crunchMessagesFromEntries(entries1).contains("message1"));
        assertTrue(crunchMessagesFromEntries(entries1).contains("message2"));
        assertTrue(crunchMessagesFromEntries(entries1).contains("message3"));

        List<StreamEntry> entries2 = mockOfJedis.xrange("stream1", e01, e12);
        assertFalse(entries2.isEmpty());
        assertTrue(crunchMessagesFromEntries(entries2).contains("message1"));
        assertFalse(crunchMessagesFromEntries(entries2).contains("message2"));
        assertFalse(crunchMessagesFromEntries(entries2).contains("message3"));

        List<StreamEntry> entries3 = mockOfJedis.xrange("stream1", e23, e3N);
        assertFalse(entries3.isEmpty());
        assertFalse(crunchMessagesFromEntries(entries3).contains("message1"));
        assertFalse(crunchMessagesFromEntries(entries3).contains("message2"));
        assertTrue(crunchMessagesFromEntries(entries3).contains("message3"));

        List<StreamEntry> entries4 = mockOfJedis.xrange("stream1", e12, null);
        assertFalse(entries4.isEmpty());
        assertFalse(crunchMessagesFromEntries(entries4).contains("message1"));
        assertTrue(crunchMessagesFromEntries(entries4).contains("message2"));
        assertTrue(crunchMessagesFromEntries(entries4).contains("message3"));

        List<StreamEntry> entries5 = mockOfJedis.xrange("stream1", null, e12);
        assertFalse(entries5.isEmpty());
        assertTrue(crunchMessagesFromEntries(entries5).contains("message1"));
        assertFalse(crunchMessagesFromEntries(entries5).contains("message2"));
        assertFalse(crunchMessagesFromEntries(entries5).contains("message3"));

    }

    @Test
    public void xread1Test() {
        StreamEntryID entryId = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("message", "message456"));
        Map<String, StreamEntryID> streams   = Map.of("stream1", StreamEntryID.LAST_ENTRY);
        List<Map.Entry<String,List<StreamEntry>>> entries = mockOfJedis.xread(new XReadParams(), streams);
        assertTrue(entries == null ||  entries.isEmpty());
    }


    @Test
    public void xread2Test() throws InterruptedException {
        Semaphore semaphore = new Semaphore(0);
        String message = "message" + ThreadLocalRandom.current().nextInt(1000);
        AtomicReference<List<Map.Entry<String,List<StreamEntry>>>> response = new AtomicReference<>();
        executorService.submit(() -> {
            Map<String, StreamEntryID> streams   = Map.of("stream1", StreamEntryID.LAST_ENTRY);
            List<Map.Entry<String,List<StreamEntry>>> entries = mockOfJedis.xread(new XReadParams(), streams);
            response.set(entries);
            semaphore.release();
        });
        Thread.sleep(200);
        StreamEntryID entryId = mockOfJedis.xadd("stream1", new XAddParams(), Map.of("message", message));
        boolean acquired = semaphore.tryAcquire(1000, TimeUnit.MILLISECONDS);
        List<Map.Entry<String,List<StreamEntry>>> entries = response.get();
        assertTrue(acquired);
        assertTrue(entries != null && !entries.isEmpty());
        assertEquals("stream1", entries.get(0).getKey());
        assertEquals(message, entries.get(0).getValue().get(0).getFields().get("message"));
    }


    private String crunchMessagesFromEntries(List<StreamEntry> entryList) {
        return entryList.stream().
                map(entry -> entry.getFields().get("message"))
                .collect(Collectors.joining(","));
    }

}
