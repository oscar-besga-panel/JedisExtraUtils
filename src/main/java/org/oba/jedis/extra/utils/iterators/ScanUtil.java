package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.JedisPoolAdapter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
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
        ScanIterable iterable = new ScanIterable(jedisPool, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
        return iterable.asList();
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
        return retrieveListOfKeys( JedisPoolAdapter.poolFromJedis(jedis), pattern);
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
        ScanIterable iterable = new ScanIterable(jedisPool, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
        iterable.forEach(action);
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
        useListOfKeys( JedisPoolAdapter.poolFromJedis(jedis), pattern, action);
    }


}
