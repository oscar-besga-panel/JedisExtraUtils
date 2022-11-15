package org.obapanel.jedis.iterators;

import org.obapanel.jedis.utils.Listable;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

import static org.obapanel.jedis.iterators.AbstractScanIterator.DEFAULT_PATTERN_ITERATORS;
import static org.obapanel.jedis.iterators.AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS;


/**
 * Iterable for zset entries (ordered set)
 * Jedis pool connection is required
 * Name of the element on redis is required
 * (if not exists, it acts like as called for an empty set)
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class ZScanIterable implements Iterable<Tuple>, Listable<Tuple> {


    private final JedisPool jedisPool;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     */
    public ZScanIterable(JedisPool jedisPool, String name) {
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     */
    public ZScanIterable(JedisPool jedisPool, String name, String pattern) {
        this(jedisPool, name, pattern, DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterable(JedisPool jedisPool, String name, int resultsPerScan) {
        this(jedisPool, name, DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPool Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterable(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
        this.jedisPool = jedisPool;
        this.name = name;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }

    @Override
    public ZScanIterator iterator() {
        return new ZScanIterator(jedisPool, name, pattern, resultsPerScan);
    }

    /**
     * This method returns ALL of the values of the iterable as a unmodificable list
     * The list is unmodificable and contains no repeated elements
     * @return list with values
     */
    public List<Tuple> asList() {
        return iterator().asList();
    }
}
