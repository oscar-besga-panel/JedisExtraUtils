package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Listable;
import redis.clients.jedis.UnifiedJedis;

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

    private final UnifiedJedis redisClient;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for redis entries
     * @param redisClient Jedis connection pool
     */
    public ScanIterable(UnifiedJedis redisClient){
        this(redisClient, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for redis entries
     * @param redisClient Jedis connection pool
     * @param pattern Pattern to be matched on the responses
     */
    public ScanIterable(UnifiedJedis redisClient, String pattern){
        this(redisClient, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for redis entries
     * @param redisClient Jedis connection pool
     * @param resultsPerScan results per call to redis
     */
    public ScanIterable(UnifiedJedis redisClient, int resultsPerScan) {
        this(redisClient, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for redis entries
     * @param redisClient Jedis connection pool
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public ScanIterable(UnifiedJedis redisClient, String pattern, int resultsPerScan) {
        this.redisClient = redisClient;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }

    @Override
    public ScanIterator iterator() {
        return new ScanIterator(redisClient, pattern, resultsPerScan);
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
