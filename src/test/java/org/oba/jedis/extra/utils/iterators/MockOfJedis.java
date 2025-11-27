package org.oba.jedis.extra.utils.iterators;

import org.mockito.Mockito;
import org.oba.jedis.extra.utils.test.TTL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.oba.jedis.extra.utils.test.TestingUtils.extractSetParamsExpireTimePX;
import static org.oba.jedis.extra.utils.test.TestingUtils.isSetParamsNX;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedis {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockOfJedis.class);

    private static final String ABC = "abcdefhijklmnopqrstuvwxyz";


    public static final String CLIENT_RESPONSE_OK = "OK";
    public static final String CLIENT_RESPONSE_KO = "KO";


    // Zero to prevent some unit test
    // One to one pass
    // More to more passes
    static final int UNIT_TEST_CYCLES = 1;

    static boolean unitTestEnabled(){
        return UNIT_TEST_CYCLES > 0;
    }

    private final JedisPooled jedisPooled;
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Map<String,String>> hdata = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Set<String>> sdata = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Set<Tuple>> zdata = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedis() {
        timer = new Timer();
        jedisPooled = Mockito.mock(JedisPooled.class);
        Mockito.when(jedisPooled.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExists(key);
        });
        Mockito.when(jedisPooled.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedisPooled.set(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = new SetParams();
            return mockSet(key, value, setParams);
        });
        Mockito.when(jedisPooled.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);
        });
        Mockito.when(jedisPooled.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDel(key);
        });
        Mockito.when(jedisPooled.scan(anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String pattern = ioc.getArgument(0);
            ScanParams scanParams = ioc.getArgument(1);
            return mockScan(pattern, scanParams);
        });
        Mockito.when(jedisPooled.scan(anyString())).thenAnswer(ioc -> {
            String pattern = ioc.getArgument(0);
            ScanParams scanParams = new ScanParams().match("*");
            return mockScan(pattern, scanParams);
        });
        Mockito.when(jedisPooled.hget(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String key = ioc.getArgument(1);
            return mockHGet(name, key);
        });
        Mockito.when(jedisPooled.hset(anyString(), anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String key = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return mockHSet(name, key, value);
        });
        Mockito.when(jedisPooled.hset(anyString(), any(Map.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            Map<String,String> values = ioc.getArgument(1);
            return mockHSet(name, values);
        });
        Mockito.when(jedisPooled.hdel(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String key = ioc.getArgument(1);
            return mockHDel(name, key);
        });
        Mockito.when(jedisPooled.hscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockHScan(name, pattern, scanParams);
        });
        Mockito.when(jedisPooled.hscan(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = new ScanParams();
            return mockHScan(name, pattern, scanParams);
        });
        Mockito.when(jedisPooled.sadd(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSAdd(name, value);
        });
        Mockito.when(jedisPooled.srem(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSRem(name, value);
        });
        Mockito.when(jedisPooled.sismember(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSIsMember(name, value);
        });
        Mockito.when(jedisPooled.sscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockSScan(name, pattern, scanParams);
        });
        Mockito.when(jedisPooled.sscan(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = new ScanParams();
            return mockSScan(name, pattern, scanParams);
        });
        Mockito.when(jedisPooled.zadd(anyString(), anyDouble(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            double score = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return mockZAdd(name, score, value);
        });
        Mockito.when(jedisPooled.zrem(anyString(), any())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            Object value = ioc.getArgument(1);
            if (value instanceof  String) {
                return mockZRem(name, new String[]{(String)value});
            } else if (value instanceof  String[]) {
                return mockZRem(name, (String[])value);
            } else {
                throw new IllegalArgumentException("Bad argument for mock zrem " +  ioc.getArgument(1));
            }
        });
        Mockito.when(jedisPooled.zscore(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockZScore(name, value);
        });
        Mockito.when(jedisPooled.zscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockZScan(name, pattern, scanParams);
        });
        Mockito.when(jedisPooled.zscan(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = new ScanParams();
            return mockZScan(name, pattern, scanParams);
        });
    }

    private Long mockZAdd(String name, double score, String value) {
        Set<Tuple> set = zdata.computeIfAbsent(name, k -> new HashSet<>());
        return set.add(new Tuple(value, score)) ? 1L : 0L;
    }

    private Long mockZRem(String name, String[] values) {
        Set<Tuple> set = zdata.computeIfAbsent(name, k -> new HashSet<>());
        AtomicLong result = new AtomicLong(0);
        for(String value: Arrays.asList(values)) {
            set.stream().
                    filter( tuple -> tuple.getElement().equals(value)).
                    findFirst().
                    ifPresent( toDel -> {
                        if (set.remove(toDel)) {
                            result.incrementAndGet();
                        }
                    });
        }
        return result.get();
    }

    private Double mockZScore(String name, String value) {
        Set<Tuple> set = zdata.computeIfAbsent(name, k -> new HashSet<>());
        Optional<Tuple> result = set.stream().
                filter( t -> value.equals(t.getElement())).
                findFirst();
        return result.isPresent() ? result.get().getScore() : null;
    }

    private ScanResult<Tuple> mockZScan(String name, String pattern, ScanParams scanParams) {
        Set<Tuple> set = zdata.computeIfAbsent(name, k -> new HashSet<>());
        List<Tuple> results = new ArrayList<>(set);
        return new ScanResult<>(ScanParams.SCAN_POINTER_START, results);
    }

    private Long mockSAdd(String name, String value) {
        Set<String> set = sdata.computeIfAbsent(name, k -> new HashSet<>());
        return set.add(value) ? 1L : 0L;
    }

    private Long mockSRem(String name, String value) {
        Set<String> set = sdata.computeIfAbsent(name, k -> new HashSet<>());
        return set.remove(value) ? 1L : 0L;
    }

    private Boolean mockSIsMember(String name, String value) {
        Set<String> set = sdata.computeIfAbsent(name, k -> new HashSet<>());
        return set.contains(value);
    }

    private ScanResult<String> mockSScan(String name, String pattern, ScanParams scanParams) {
        Set<String> set = sdata.computeIfAbsent(name, k -> new HashSet<>());
        List<String> results = new ArrayList<>(set);
        return new ScanResult<>(ScanParams.SCAN_POINTER_START, results);
    }

    private String mockHGet(String name, String key) {
        Map<String, String> map = hdata.computeIfAbsent(name, k ->  new HashMap<>());
        return map.get(key);
    }

    private Long mockHSet(String name, String key, String value) {
        Map<String, String> map = hdata.computeIfAbsent(name, k ->  new HashMap<>());
        map.put(key, value);
        return 0L;
    }

    private Long mockHSet(String name,Map<String,String> values) {
        Map<String, String> map = hdata.computeIfAbsent(name, k ->  new HashMap<>());
        map.putAll(values);
        return 0L;
    }

    private Long mockHDel(String name, String key) {
        Map<String, String> map = hdata.computeIfAbsent(name, k ->  new HashMap<>());
        return map.remove(key) != null ? 1L : 0L;
    }

    private ScanResult<Map.Entry<String,String>> mockHScan(String name, String pattern, ScanParams scanParams) {
        Map<String, String> map = hdata.computeIfAbsent(name, k ->  new HashMap<>());
        List<Map.Entry<String,String>> results = new ArrayList<>(map.entrySet());
        return new ScanResult<Map.Entry<String,String>>(ScanParams.SCAN_POINTER_START, results);
    }

//    private ScanResult<String> mockScan(String pattern, ScanParams scanParams) {
//        List<String> results = new ArrayList<>(data.keySet());
//
//        return new ScanResult<String>(ScanParams.SCAN_POINTER_START, results);
//    }

    private ScanResult<String> mockScan(String cursor, ScanParams scanParams) {
        if (!cursor.equals("0")) {
            LOGGER.warn("Cursor inited wirh value {}", cursor);
        }
        String pattern = extractPatternFromScanParams(scanParams);
        List<String> keys = data.keySet().stream().
                filter( k -> k.matches(pattern) ).
                collect(Collectors.toList());
        return new ScanResult<>(ScanParams.SCAN_POINTER_START, keys);
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

    private synchronized Long mockDel(String key) {
        if (data.containsKey(key)) {
            data.remove(key);
            return 1L;
        } else if (hdata.containsKey(key)) {
            hdata.remove(key);
            return 1L;
        } else if (sdata.containsKey(key)) {
            sdata.remove(key);
            return 1L;
        } else if (zdata.containsKey(key)) {
            zdata.remove(key);
            return 1L;
        } else {
            return 0L;
        }
    }

    private synchronized boolean mockExists(String key) {
        return data.containsKey(key) ||
                hdata.containsKey(key) ||
                sdata.containsKey(key) ||
                zdata.containsKey(key);
    }


    public JedisPooled getJedisPooled(){
        return jedisPooled;
    }

    public synchronized void clearData(){
        data.clear();
        hdata.clear();
        sdata.clear();
        zdata.clear();
    }

    public synchronized Map<String,String> getCurrentData() {
        return new HashMap<>(data);
    }

    public List<String> randomSizedListOfChars() {
        int size = ThreadLocalRandom.current().nextInt(5, ABC.length());
        List<String> result = new ArrayList<>(size);
        for(int i=0; i < size ; i++) {
            result.add(String.valueOf(ABC.toCharArray()[i]));
        }
        return result;
    }

    public static String extractPatternFromScanParams(ScanParams scanParams) {
        String pattern = scanParams.match();
        if (pattern == null) {
            pattern = "";
        } else if (pattern.equals("*")) {
            pattern = ".*";
        } else if (pattern.endsWith("*")) {
            pattern = pattern.replace("*",".*");
        }
        return pattern;
    }

}
