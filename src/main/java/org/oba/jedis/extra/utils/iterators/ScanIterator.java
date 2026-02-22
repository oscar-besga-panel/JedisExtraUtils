package org.oba.jedis.extra.utils.iterators;

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * Iterator scan for the keys of the redis database
 * Only one use for an instace of this class
 * Jedis pool connection is required
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public final class ScanIterator extends AbstractScanIterator<String> {

    /**
     * Creates a new only-one-use iterator
     * @param redisClient Connection pool
     */
    public ScanIterator(UnifiedJedis redisClient) {
        this(redisClient, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Creates a new only-one-use iterator
     * @param redisClient Connection pool
     * @param pattern Patter to be used as filter
     */
    public ScanIterator(UnifiedJedis redisClient, String pattern) {
        this(redisClient, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Creates a new only-one-use iterator
     * @param redisClient Connection pool
     * @param resultsPerScan Result that will return in each scan (hopefully)
     */
    public ScanIterator(UnifiedJedis redisClient, int resultsPerScan) {
        this(redisClient, DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Creates a new only-one-use iterator
     * @param redisClient Connection pool
     * @param pattern Patter to be used as filter
     * @param resultsPerScan Result that will return in each scan (hopefully)
     */
    public ScanIterator(UnifiedJedis redisClient, String pattern, int resultsPerScan) {
        super(redisClient, pattern, resultsPerScan);
    }


    @Override
    ScanResult<String> doScan(UnifiedJedis redisClient, String currentCursor, ScanParams scanParams) {
        return redisClient.scan(currentCursor, scanParams);
    }

    @Override
    void doRemove(UnifiedJedis redisClient, String next) {
        redisClient.del(next);
    }
}
