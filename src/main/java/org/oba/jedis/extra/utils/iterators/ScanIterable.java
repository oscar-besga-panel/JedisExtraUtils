package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Listable;
import redis.clients.jedis.JedisPooled;

import java.util.List;

/**
 * Iterable for redis entries
 * Jedis pool connection is required
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class ScanIterable implements Iterable<String>, Listable<String> {

    private final JedisPooled jedisPooled;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for redis entries
     * @param jedisPooled Jedis connection pool
     */
    public ScanIterable(JedisPooled jedisPooled){
        this(jedisPooled, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for redis entries
     * @param jedisPooled Jedis connection pool
     * @param pattern Pattern to be matched on the responses
     */
    public ScanIterable(JedisPooled jedisPooled, String pattern){
        this(jedisPooled, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for redis entries
     * @param jedisPooled Jedis connection pool
     * @param resultsPerScan results per call to redis
     */
    public ScanIterable(JedisPooled jedisPooled, int resultsPerScan) {
        this(jedisPooled, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for redis entries
     * @param jedisPooled Jedis connection pool
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public ScanIterable(JedisPooled jedisPooled, String pattern, int resultsPerScan) {
        this.jedisPooled = jedisPooled;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }

    @Override
    public ScanIterator iterator() {
        return new ScanIterator(jedisPooled, pattern, resultsPerScan);
    }

    /**
     * This method returns ALL of the values of the iterable as a unmodificable list
     * The list is unmodificable and contains no repeated elements
     * @return list with values
     */
    @Override
    public List<String> asList() {
        return iterator().asList();
    }
}
