package org.oba.jedis.extra.utils.notificationLock;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.lock.IJedisLock;
import org.oba.jedis.extra.utils.test.TTL;
import org.oba.jedis.extra.utils.test.TransactionOrder;
import org.oba.jedis.extra.utils.utils.ScriptEvalSha1;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.oba.jedis.extra.utils.test.TestingUtils.extractSetParamsExpireTimePX;
import static org.oba.jedis.extra.utils.test.TestingUtils.isSetParamsNX;
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
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final List<TransactionOrder<String>> transactionActions = new ArrayList<>();
    private final Timer timer;

    public MockOfJedis() {
        PowerMockito.suppress(MemberMatcher.methodsDeclaredIn(TransactionBase.class));
        timer = new Timer();
        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        Transaction transaction = PowerMockito.mock(Transaction.class);
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
        Mockito.when(jedis.scriptLoad(anyString())).thenAnswer( ioc -> {
            String script = ioc.getArgument(0, String.class);
            return ScriptEvalSha1.sha1(script);
        });
        Mockito.when(jedis.evalsha(anyString(), any(List.class), any(List.class))).thenAnswer( ioc -> {
            String name = ioc.getArgument(0, String.class);
            List<String> keys = ioc.getArgument(1, List.class);
            List<String> args = ioc.getArgument(2, List.class);
            return mockEvalsha(keys, args);
        });
        Mockito.when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);

        });
        Mockito.when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedis.multi()).thenReturn(transaction);
        Mockito.when(transaction.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockTransactionGet(key);
        });
        Mockito.when(transaction.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockTransactionSet(key, value, setParams);
        });
        Mockito.when(transaction.exec()).thenAnswer(ioc -> mockTransactionExec());
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
                Thread.sleep(count * 5L);
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

    private synchronized Object mockEvalsha(List<String> keys, List<String> values) {
        Object response = null;
        if (values.get(0).equalsIgnoreCase(data.get(keys.get(0))) ){
            String removed = data.remove(keys.get(0));
            response = removed != null ? 1 : 0;
        }
        return response;
    }

    private synchronized String mockGet(String key) {
        return data.get(key);
    }

    private synchronized String mockSet(final String key, String value, SetParams setParams) {
        boolean insert = true;
        if (isSetParamsNX(setParams)) {
            insert = !data.containsKey(key);
        }
        if (insert) {
            data.put(key, value);
            Long expireTime = extractSetParamsExpireTimePX(setParams);
            if (expireTime != null){
                timer.schedule(TTL.wrapTTL(() -> data.remove(key)),expireTime);
            }
            return  CLIENT_RESPONSE_OK;
        } else {
            return  CLIENT_RESPONSE_KO;
        }
    }

    private synchronized Response<String> mockTransactionGet(String key){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockGet(key));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized Response<String> mockTransactionSet(String key, String value, SetParams setParams){
        TransactionOrder<String> transactionOrder = new TransactionOrder<>(() -> mockSet(key, value, setParams));
        transactionActions.add(transactionOrder);
        return transactionOrder.getResponse();
    }

    private synchronized List<Object> mockTransactionExec(){
        transactionActions.forEach(TransactionOrder::execute);
        List<Object> responses = transactionActions.stream().
                map(TransactionOrder::getResponse).
                collect(Collectors.toList());
        transactionActions.clear();
        return responses;
    }

    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }

    public synchronized void clearData(){
        streamData.clear();
        data.clear();
    }

    public Map<String, Map<StreamEntryID, Map<String,String>>> getCurrentStreamData() {
        return streamData;
    }

    public Map<String, String> getCurrentData() {
        return data;
    }

    static boolean checkLock(IJedisLock jedisLock){
        LOGGER.info("interruptingLock.isLocked() " + jedisLock.isLocked() + " for thread " + Thread.currentThread().getName());
        if (jedisLock.isLocked()) {
            LOGGER.debug("LOCKED");
            return true;
        } else {
            IllegalStateException ise =  new IllegalStateException("LOCK NOT ADQUIRED isLocked " + jedisLock.isLocked());
            LOGGER.error("ERROR LOCK NOT ADQUIRED e {} ", ise.getMessage(), ise);
            throw ise;
        }
    }

}
