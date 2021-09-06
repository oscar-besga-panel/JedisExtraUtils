package org.obapanel.jedis.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This class can help with the key scanning
 * It can use a pool or a single connection
 * Also, it can return a list or execute a method for each result
 *
 * CAUTION: in the last mode, REDIS specifies that a SCAN operation can return duplicates
 * this is very rare but possible, your code must handle it or be aware of this
 * https://redis.io/commands/scan
 *
 * The list result has filtered duplicates
 *
 * NOTE: feel free to contribute adding HSCAN, SSCAN, ZSCAN
 */
public class ScanUtil {


    /**
     * Scans for keys given a pattern
     * This method avoid returning duplicates
     *
     * @param jedisPool Pool of connections
     * @param pattern Patter for keys to match
     * @return List of matching keys
     */
    public static List<String> retrieveListOfKeys(JedisPool jedisPool, String pattern) {
        try (Jedis jedis = jedisPool.getResource()) {
            return retrieveListOfKeys(jedis, pattern);
        }
    }

    /**
     * Scans for keys given a pattern
     * This method avoid returning duplicates
     *
     * @param jedis Connection
     * @param pattern Patter for keys to match
     * @return List of matching keys
     */
    public static List<String> retrieveListOfKeys(Jedis jedis, String pattern) {
        Set<String> listOfKeys = new HashSet<>();
        ScanParams scanParams = new ScanParams().match(pattern); // Scan on two-by-two responses
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> partialResult =  jedis.scan(cursor, scanParams);
            cursor = partialResult.getCursor();
            listOfKeys.addAll(partialResult.getResult());
        }  while(!cursor.equals(ScanParams.SCAN_POINTER_START));
        return new ArrayList<>(listOfKeys);
    }

    /**
     * Scans for keys given a pattern
     * CAUTION! This method could return duplicates (although very rarely)
     *
     * @param jedisPool Pool of connections
     * @param pattern Patter for keys to match
     * @param action executed for each key
     */
    public static void useListOfKeys(JedisPool jedisPool, String pattern, Consumer<String> action) {
        try (Jedis jedis = jedisPool.getResource()) {
            useListOfKeys(jedis, pattern, action);
        }
    }

    /**
     * Scans for keys given a pattern
     * CAUTION! This method could return duplicates (although very rarely)
     *
     * @param jedis Connection
     * @param pattern Patter for keys to match
     * @param action executed for each key
     */
    public static void useListOfKeys(Jedis jedis, String pattern, Consumer<String> action) {
        ScanParams scanParams = new ScanParams().match(pattern); // Scan on two-by-two responses
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> partialResult =  jedis.scan(cursor, scanParams);
            cursor = partialResult.getCursor();
            partialResult.getResult().
                    forEach(action);
        }  while(!cursor.equals(ScanParams.SCAN_POINTER_START));
    }


}
