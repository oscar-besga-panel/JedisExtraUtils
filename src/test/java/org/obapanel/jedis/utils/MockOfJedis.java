package org.obapanel.jedis.utils;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.obapanel.jedis.common.test.TTL.wrapTTL;

/**
 * Mock of jedis methods used by the lock
 */
public class MockOfJedis {

    private static final Logger LOG = LoggerFactory.getLogger(MockOfJedis.class);

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
    private final Timer timer;

    public MockOfJedis() {
        timer = new Timer();

        jedis = Mockito.mock(Jedis.class);
        jedisPool = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool.getResource()).thenReturn(jedis);

        Mockito.when(jedis.exists(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockExist(key);
        });
        Mockito.when(jedis.get(anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            return mockGet(key);
        });
        Mockito.when(jedis.set(anyString(), anyString())).thenAnswer(ioc -> {
            String key = ioc.getArgument(0);
            String value = ioc.getArgument(1);
            return mockSet(key, value, null);

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
            String cursor = ioc.getArgument(0);
            ScanParams scanParams = ioc.getArgument(1);
            return mockScan(cursor, scanParams);
        });

    }

    private boolean mockExist(String key) {
        return data.containsKey(key);
    }


    private ScanResult<String> mockScan(String cursor, ScanParams scanParams) {
        String pattern = extractPatternFromScanParams(scanParams);
        List<String> keys = data.keySet().stream().
                filter( k -> k.matches(pattern) ).
                collect(Collectors.toList());
        return new ScanResult<>("0", keys);
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
        } else {
            return 0L;
        }
    }

    public Jedis getJedis(){
        return jedis;
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }

    public synchronized void clearData(){
        data.clear();
    }


    public synchronized Map<String,String> getCurrentData() {
        return new HashMap<>(data);
    }

    boolean isSetParamsNX(SetParams setParams) {
        boolean result = false;
        if (setParams != null) {
            for (byte[] b : setParams.getByteParams()) {
                String s = new String(b);
                if ("nx".equalsIgnoreCase(s)) {
                    result = true;
                    break;
                }
            }
        }
        return result;
    }

    Long getExpireTimePX(SetParams setParams) {
        return setParams != null ? setParams.getParam("px") : null;
    }

    public static String extractPatternFromScanParams(ScanParams scanParams) {
        boolean nextIsPattern = false;
        String pattern = "";
        for(byte[] p : scanParams.getParams()) {
            String s = new String(p).intern();
            if (nextIsPattern) {
                pattern = s;
            }
            nextIsPattern = Protocol.Keyword.MATCH.name().equalsIgnoreCase(s);
        }
        if (pattern.equals("*")) {
            pattern = ".*";
        }
        return pattern;
    }

 }
