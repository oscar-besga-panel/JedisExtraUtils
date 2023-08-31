package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Listable;
import redis.clients.jedis.JedisPool;

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
public class ScanIterable implements Iterable<String>, Listable<String>, JedisPoolUser {

    private final JedisPool jedisPool;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for redis entries
     * @param jedisPool Jedis connection pool
     */
    public ScanIterable(JedisPool jedisPool){
        this(jedisPool, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for redis entries
     * @param jedisPool Jedis connection pool
     * @param pattern Pattern to be matched on the responses
     */
    public ScanIterable(JedisPool jedisPool, String pattern){
        this(jedisPool, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for redis entries
     * @param jedisPool Jedis connection pool
     * @param resultsPerScan results per call to redis
     */
    public ScanIterable(JedisPool jedisPool, int resultsPerScan) {
        this(jedisPool, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for redis entries
     * @param jedisPool Jedis connection pool
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public ScanIterable(JedisPool jedisPool, String pattern, int resultsPerScan) {
        this.jedisPool = jedisPool;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }

    @Override
    public ScanIterator iterator() {
        return new ScanIterator(jedisPool, pattern, resultsPerScan);
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
