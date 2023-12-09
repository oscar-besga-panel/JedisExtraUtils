package org.oba.jedis.extra.utils.notificationLock;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.powermock.api.mockito.PowerMockito.when;

public class MockOfJedis {


    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedis.class);

    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int UNIT_TEST_CYCLES = 1;

    static boolean unitTestEnabled(){
        return UNIT_TEST_CYCLES > 0;
    }

    private final JedisPool jedisPool;
    private final Jedis jedis;
    private final Map<String, Map<StreamEntryID, Map<String,String>>> streamData = Collections.synchronizedMap(new HashMap<>());

    public MockOfJedis() {
        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.xrange(anyString(), (StreamEntryID)isNull(), (StreamEntryID) isNull())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0, String.class);
            return xrange(name, null, null);
        });
        when(jedis.xrange(anyString(), any(StreamEntryID.class), isNull())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0, String.class);
            StreamEntryID start = ioc.getArgument(1, StreamEntryID.class);
            return xrange(name, start, null);
        });
        when(jedis.xrange(anyString(), isNull(), any(StreamEntryID.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0, String.class);
            StreamEntryID end = ioc.getArgument(1, StreamEntryID.class);
            return xrange(name, null, end);
        });
        when(jedis.xrange(anyString(), any(StreamEntryID.class), any(StreamEntryID.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0, String.class);
            StreamEntryID start = ioc.getArgument(1, StreamEntryID.class);
            StreamEntryID end = ioc.getArgument(2, StreamEntryID.class);
            return xrange(name, start, end);
        });
        when(jedis.xread(any(XReadParams.class), any(Map.class))).thenAnswer( ioc -> {
            XReadParams xReadParams = ioc.getArgument(0, XReadParams.class);
            Map<String, StreamEntryID> streams = (Map<String, StreamEntryID>) ioc.getArgument(1, Map.class);
            return xread(xReadParams, streams);
        });
        when(jedis.xadd(anyString(), any(XAddParams.class), any(Map.class))).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            XAddParams addParams = ioc.getArgument(1, XAddParams.class);
            Map<String, String> data = (Map<String, String>) ioc.getArgument(2, Map.class);
            return xadd(name, addParams, data);
        });
    }

    List<Map.Entry<String, List<StreamEntry>>> xread(XReadParams xReadParams, Map<String, StreamEntryID> streams) {
        Map<String, List<StreamEntry>> result = new HashMap<>();
        // We asume that xReadParmas has a 1 COUNT and 5000 block
        long tsMax = System.currentTimeMillis() + 5000;
        int count = 0;
        StreamEntryID streamEntryIDOfNow = newStreamIdOfNow();
        while (result.isEmpty() && (tsMax > System.currentTimeMillis())) {
            streams.forEach((streamName, startStreamEntryID) -> {
                StreamEntryID effectiveStart;
                if (startStreamEntryID.equals(StreamEntryID.LAST_ENTRY)) {
                    effectiveStart = streamEntryIDOfNow;
                } else {
                    effectiveStart = startStreamEntryID;
                }
                // ufff
                List<StreamEntry> resultForStream = xreadStream(streamName, effectiveStart);
                if (resultForStream != null && !resultForStream.isEmpty()) {
                    result.put(streamName, resultForStream);
                }
            });
            try {
                Thread.sleep(count * 5);
                count++;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return result.isEmpty() ? null : new ArrayList<>(result.entrySet());
    }

    List<StreamEntry> xreadStream(String streamName, StreamEntryID start) {
        streamData.computeIfAbsent(streamName, k ->  new HashMap<>());
        StreamEntryID effectiveStart;
        if (start.equals(StreamEntryID.LAST_ENTRY)) {
            effectiveStart = newStreamIdOfNow();
            LOGGER.debug("effectiveStart for $ is {}", effectiveStart);
        } else {
            effectiveStart = start;
            LOGGER.debug("effectiveStart is {}", effectiveStart);
        }
        return streamData.get(streamName).entrySet().stream().
                filter(sentry -> isEntryBetween(sentry.getKey(), effectiveStart)).
                map(this::convertToStreamEntry).
                collect(Collectors.toList());
    }


    StreamEntryID xadd(String name, XAddParams addParams, Map<String, String> data) {
        streamData.computeIfAbsent(name, k ->  new HashMap<>());
        StreamEntryID streamEntryID = newStreamId(streamData.get(name).keySet());
        streamData.get(name).put(streamEntryID, data);
        return streamEntryID;
    }

    synchronized StreamEntryID newStreamIdOfNow(){
        return newStreamId(Set.of());
    }


    synchronized StreamEntryID newStreamId(Set<StreamEntryID> streamEntryIDSet){
        long ts = System.currentTimeMillis();
        int sequence = 0;
        StreamEntryID streamEntryID = new StreamEntryID(String.format("%d-%d", ts, sequence));
        while (streamEntryIDSet.contains(streamEntryID)) {
            sequence++;
            streamEntryID = new StreamEntryID(String.format("%d-%d", ts, sequence));
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return streamEntryID;
    }

    List<StreamEntry> xrange(String name, StreamEntryID start, StreamEntryID end) {
        streamData.computeIfAbsent(name, k ->  new HashMap<>());
        return streamData.get(name).entrySet().stream().
                filter(entry -> isEntryBetween(entry.getKey(), start, end)).
                map(this::convertToStreamEntry).
                collect(Collectors.toList());
    }


    boolean isEntryBetween(StreamEntryID entry, StreamEntryID start) {
        return isEntryBetween(entry, start, null);
    }

    boolean isEntryBetween(StreamEntryID entry, StreamEntryID start, StreamEntryID end) {
        boolean between = true;
        if (end != null) {
            between = entry.getTime() < end.getTime() ||
                    (entry.getTime() == end.getTime() && entry.getSequence() <= end.getSequence());
        }
        // StreamEntry.$ is meant for an streamEntry of now
        if (start != null && start.equals(StreamEntryID.LAST_ENTRY)) {
            start = newStreamIdOfNow();
        }
        if (between && start != null) {
            between = start.equals(StreamEntryID.LAST_ENTRY) ||
                    ( entry.getTime() > start.getTime() ||
                    (entry.getTime() == start.getTime() && entry.getSequence() >= start.getSequence()));
        }
        return between;
    }

    StreamEntry convertToStreamEntry(Map.Entry<StreamEntryID, Map<String, String>> entry) {
        return new StreamEntry(entry.getKey(), entry.getValue());
    }

    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }

    public synchronized void clearData(){
        streamData.clear();
    }

    public Map<String, Map<StreamEntryID, Map<String,String>>> getData() {
        return streamData;
    }



}
