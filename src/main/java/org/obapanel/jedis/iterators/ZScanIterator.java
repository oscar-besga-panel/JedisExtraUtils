package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

/**
 * Iterator for zset entries
 * Only one use for an instace of this class
 *
 * Jedis pool connection is required
 * Name of the element on redis is required
 * (if not exists, it acts like as called for an empty set)
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class ZScanIterator extends AbstractScanIterator<Tuple>  {


    private static final Logger LOGGER = LoggerFactory.getLogger(ZScanIterator.class);

    private final String name;

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     */
    public ZScanIterator(JedisPool jedisPool, String name) {
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     */
    public ZScanIterator(JedisPool jedisPool, String name, String pattern) {
        this(jedisPool, name, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterator(JedisPool jedisPool, String name, int resultsPerScan) {
        this(jedisPool, name,DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterator for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
        super(jedisPool, pattern, resultsPerScan);
        this.name = name;
    }

    @Override
    ScanResult<Tuple> doScan(Jedis jedis, String currentCursor, ScanParams scanParams) {
        return jedis.zscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(Jedis jedis, Tuple next) {
        jedis.zrem(name, next.getElement());
    }


}
