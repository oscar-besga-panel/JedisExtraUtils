package org.oba.jedis.extra.utils.cache;

import org.oba.jedis.extra.utils.iterators.ScanIterator;
import org.oba.jedis.extra.utils.utils.Listable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterator that travels by all keys of the cache.
 * Every entry can cause two operations on jedis, one for scan and other for get
 */
public final class CacheKeyIterator implements Iterator<String>, Listable<String> {

    public static final int DEFAULT_RESULTS_PER_SCAN_ITERATORS = 50;

    private static final boolean DEFAULT_RESOLVE = true;

    private final SimpleCache cache;
    private final ScanIterator scanIterator;

    private String currentKey;

    private final boolean resolve;

    /**
     * Internal constructor
     * @param cache Cache where the iterator belongs
     */
    CacheKeyIterator(SimpleCache cache) {
        this(cache, DEFAULT_RESOLVE);
    }

    /**
     * Internal constructor
     * @param cache Cache where the iterator belongs
     * @param resolve if names should be resolved or recovered as are in redis
     */
    CacheKeyIterator(SimpleCache cache, boolean resolve) {
        this.cache = cache;
        this.resolve = resolve;
        this.scanIterator = new ScanIterator(cache.getJedisPool(), cache.resolveKey("*"), DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    @Override
    public boolean hasNext() {
        currentKey = null;
        return scanIterator.hasNext();
    }

    @Override
    public String next() {
        currentKey = scanIterator.next();
        if (currentKey != null) {
            if (resolve) {
                currentKey = cache.unresolveKey(currentKey);
            }
            return currentKey;
        } else {
            throw new NoSuchElementException("No next key value");
        }
    }

    /**
     * Get the value associated with the current key accessed by the iterator
     * YOU MUST CALL A VALID next() to function properly
      * @return value associated by current key
     */
    public String nextValue() {
        if (currentKey != null) {
            return cache.get(currentKey);
        } else {
            throw new NoSuchElementException("No next associated value");
        }
    }

    /**
     * This method returns ALL of the values of the iterator as a unmodificable list
     * This method consumes the iterator, no next and hasNetx method shoould be called
     * The list is unmodificable with no duplicates
     * @return list with values
     */
    public List<String> asList(){
        final Set<String> set = new HashSet<>();
        forEachRemaining(set::add);
        return Collections.unmodifiableList(new ArrayList<>(set));
    }



}
