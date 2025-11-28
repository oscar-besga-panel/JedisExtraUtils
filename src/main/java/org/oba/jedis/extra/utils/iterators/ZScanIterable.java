package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Listable;
import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.resps.Tuple;

import java.util.List;


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
public class ZScanIterable implements Iterable<Tuple>, Listable<Tuple>, Named {


    private final JedisPooled jedisPooled;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     */
    public ZScanIterable(JedisPooled jedisPooled, String name) {
        this(jedisPooled, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     */
    public ZScanIterable(JedisPooled jedisPooled, String name, String pattern) {
        this(jedisPooled, name, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterable(JedisPooled jedisPooled, String name, int resultsPerScan) {
        this(jedisPooled, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for zset entries (ordered set)
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public ZScanIterable(JedisPooled jedisPooled, String name, String pattern, int resultsPerScan) {
        this.jedisPooled = jedisPooled;
        this.name = name;
        this.pattern = pattern;
        this.resultsPerScan = resultsPerScan;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ZScanIterator iterator() {
        return new ZScanIterator(jedisPooled, name, pattern, resultsPerScan);
    }

    /**
     * This method returns ALL of the values of the iterable as a unmodificable list
     * The list is unmodificable and contains no repeated elements
     * @return list with values
     */
    @Override
    public List<Tuple> asList() {
        return iterator().asList();
    }
}
