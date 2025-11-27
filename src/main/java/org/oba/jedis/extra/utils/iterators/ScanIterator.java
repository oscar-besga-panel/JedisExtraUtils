package org.oba.jedis.extra.utils.iterators;

import redis.clients.jedis.JedisPooled;
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
     * @param jedisPooled Connection pool
     */
    public ScanIterator(JedisPooled jedisPooled) {
        this(jedisPooled, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Creates a new only-one-use iterator
     * @param jedisPooled Connection pool
     * @param pattern Patter to be used as filter
     */
    public ScanIterator(JedisPooled jedisPooled, String pattern) {
        this(jedisPooled, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Creates a new only-one-use iterator
     * @param jedisPooled Connection pool
     * @param resultsPerScan Result that will return in each scan (hopefully)
     */
    public ScanIterator(JedisPooled jedisPooled, int resultsPerScan) {
        this(jedisPooled, DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Creates a new only-one-use iterator
     * @param jedisPooled Connection pool
     * @param pattern Patter to be used as filter
     * @param resultsPerScan Result that will return in each scan (hopefully)
     */
    public ScanIterator(JedisPooled jedisPooled, String pattern, int resultsPerScan) {
        super(jedisPooled, pattern, resultsPerScan);
    }


    @Override
    ScanResult<String> doScan(JedisPooled jedisPooled, String currentCursor, ScanParams scanParams) {
        return jedisPooled.scan(currentCursor, scanParams);
    }

    @Override
    void doRemove(JedisPooled jedisPooled, String next) {
        jedisPooled.del(next);
    }
}
