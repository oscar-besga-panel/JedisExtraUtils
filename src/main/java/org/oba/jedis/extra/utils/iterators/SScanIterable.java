package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.Listable;
import org.oba.jedis.extra.utils.utils.Named;
import redis.clients.jedis.JedisPooled;

import java.util.List;

/**
 * Iterable for sset entries
 * Jedis pool connection is required
 * Name of the element on redis is required
 * (if not exists, it acts like as called for an empty set)
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class SScanIterable implements Iterable<String>, Listable<String>, Named {


    private final JedisPooled jedisPooled;
    private final String name;
    private final String pattern;
    private final int resultsPerScan;

    /**
     * Iterable for set entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     */
    public SScanIterable(JedisPooled jedisPooled, String name){
        this(jedisPooled, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for set entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     */
    public SScanIterable(JedisPooled jedisPooled, String name, String pattern){
        this(jedisPooled, name, pattern, AbstractScanIterator.DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    /**
     * Iterable for sset entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param resultsPerScan results per call to redis
     */
    public SScanIterable(JedisPooled jedisPooled, String name, int resultsPerScan) {
        this(jedisPooled, name, AbstractScanIterator.DEFAULT_PATTERN_ITERATORS, resultsPerScan);
    }

    /**
     * Iterable for sset entries
     * @param jedisPooled Jedis connection pool
     * @param name Name of the set
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public SScanIterable(JedisPooled jedisPooled, String name, String pattern, int resultsPerScan) {
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
    public SScanIterator iterator() {
        return new SScanIterator(jedisPooled, name, pattern, resultsPerScan);
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
