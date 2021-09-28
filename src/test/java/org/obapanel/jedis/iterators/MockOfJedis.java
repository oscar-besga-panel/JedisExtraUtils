package org.obapanel.jedis.iterators;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.*;
import static org.obapanel.jedis.common.test.TTL.wrapTTL;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedis {

    private static final Logger LOG = LoggerFactory.getLogger(MockOfJedis.class);

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

    private final JedisPool jedisPool;
    private final Jedis jedis;
    private final Map<String, String> data = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Map<String,String>> hdata = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Set<String>> sdata = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Set<Tuple>> zdata = Collections.synchronizedMap(new HashMap<>());
    private final Timer timer;

    public MockOfJedis() {
        timer = new Timer();
        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);
        Mockito.when(jedis.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExists(key);
        });
        Mockito.when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedis.set(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = new SetParams();
            return mockSet(key, value, setParams);
        });
        Mockito.when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            SetParams setParams = ioc.getArgument(2);
            return mockSet(key, value, setParams);
        });
        Mockito.when(jedis.del(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockDel(key);
        });
        Mockito.when(jedis.scan(anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String pattern = ioc.getArgument(0);
            ScanParams scanParams = ioc.getArgument(1);
            return mockScan(pattern, scanParams);
        });
        Mockito.when(jedis.scan(anyString())).thenAnswer(ioc -> {
            String pattern = ioc.getArgument(0);
            ScanParams scanParams = new ScanParams();
            return mockScan(pattern, scanParams);
        });
        Mockito.when(jedis.hget(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String key = ioc.getArgument(1);
            return mockHGet(name, key);
        });
        Mockito.when(jedis.hset(anyString(), anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String key = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return mockHSet(name, key, value);
        });
        Mockito.when(jedis.hset(anyString(), any(Map.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            Map<String,String> values = ioc.getArgument(1);
            return mockHSet(name, values);
        });
        Mockito.when(jedis.hdel(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String key = ioc.getArgument(1);
            return mockHDel(name, key);
        });
        Mockito.when(jedis.hscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockHScan(name, pattern, scanParams);
        });
        Mockito.when(jedis.hscan(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = new ScanParams();
            return mockHScan(name, pattern, scanParams);
        });
        Mockito.when(jedis.sadd(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSAdd(name, value);
        });
        Mockito.when(jedis.srem(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSRem(name, value);
        });
        Mockito.when(jedis.sismember(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSIsMember(name, value);
        });
        Mockito.when(jedis.sscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockSScan(name, pattern, scanParams);
        });
        Mockito.when(jedis.sscan(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = new ScanParams();
            return mockSScan(name, pattern, scanParams);
        });
        Mockito.when(jedis.zadd(anyString(), anyDouble(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            double score = ioc.getArgument(1);
            String value = ioc.getArgument(2);
            return mockZAdd(name, score, value);
        });
        Mockito.when(jedis.zrem(anyString(), any())).thenAnswer(ioc -> {
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
        Mockito.when(jedis.zscore(anyString(), anyString())).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockZScore(name, value);
        });
        Mockito.when(jedis.zscan(anyString(), anyString(), any(ScanParams.class))).thenAnswer(ioc -> {
            String name = ioc.getArgument(0);
            String pattern = ioc.getArgument(1);
            ScanParams scanParams = ioc.getArgument(2);
            return mockZScan(name, pattern, scanParams);
        });
        Mockito.when(jedis.zscan(anyString(), anyString())).thenAnswer(ioc -> {
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

    private ScanResult<String> mockScan(String pattern, ScanParams scanParams) {
        List<String> results = new ArrayList<>(data.keySet());
        return new ScanResult<String>(ScanParams.SCAN_POINTER_START, results);
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
            Long expireTime = getExpireTimePX(setParams);
            if (expireTime != null){
                timer.schedule(wrapTTL(() -> data.remove(key)),expireTime);
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

    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
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

    boolean isSetParamsNX(SetParams setParams) {
        boolean result = false;
        for(byte[] b: setParams.getByteParams()){
            String s = new String(b);
            if ("nx".equalsIgnoreCase(s)){
                result = true;
                break;
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return setParams.getParam("px");
    }

    public List<String> randomSizedListOfChars() {
        int size = ThreadLocalRandom.current().nextInt(5, ABC.length());
        List<String> result = new ArrayList<>(size);
        for(int i=0; i < size ; i++) {
            result.add(String.valueOf(ABC.toCharArray()[i]));
        }
        return result;
    }

}
