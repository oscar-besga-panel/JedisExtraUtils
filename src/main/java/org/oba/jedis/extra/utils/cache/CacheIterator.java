package org.oba.jedis.extra.utils.cache;

import org.oba.jedis.extra.utils.iterators.ScanIterator;
import org.oba.jedis.extra.utils.utils.Listable;
import org.oba.jedis.extra.utils.utils.Mapeable;
import org.oba.jedis.extra.utils.utils.SimpleEntry;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Iterator that travels by all pairs of key-values of the cache.
 * Every entry can cause two operations on jedis, one for scan and other for get
 */
public final class CacheIterator implements Iterator<Map.Entry<String, String>>,
        Listable<Map.Entry<String, String>>, Mapeable<String, String> {

    public static final int DEFAULT_RESULTS_PER_SCAN_ITERATORS = 50;


    private final SimpleCache cache;
    private final ScanIterator scanIterator;

    /**
     * Internal constructor
     * @param cache Cache where the iterator belongs
     */
    CacheIterator(SimpleCache cache) {
        this.cache = cache;
        this.scanIterator = new ScanIterator(cache.getJedisPool(), cache.resolveKey("*"), DEFAULT_RESULTS_PER_SCAN_ITERATORS);
    }

    @Override
    public boolean hasNext() {
        return scanIterator.hasNext();
    }

    @Override
    public Map.Entry<String, String> next() {
        String redisKey = scanIterator.next();
        if (redisKey != null) {
            try(Jedis jedis = cache.getJedisPool().getResource()) {
                String value = jedis.get(redisKey);
                String key = cache.unresolveKey(redisKey);
                return new SimpleEntry(key, value);
            }
        } else {
            throw new NoSuchElementException("No next value");
        }
    }

    /**
     * This method returns ALL values of the iterator as an unmodifiable list
     * This method consumes the iterator, no next nor hasNext method should be called
     * The list is unmodifiable with no duplicates
     * @return list with entries
     */
    public List<Map.Entry<String, String>> asList(){
        final Set<Map.Entry<String, String>> set = new HashSet<>();
        forEachRemaining(set::add);
        return Collections.unmodifiableList(new ArrayList<>(set));
    }

    /**
     * This method returns ALL values of the iterator as an unmodifiable map
     * This method consumes the iterator, no next nor hasNext method should be called
     * The map is unmodifiable with no repeated elements
     * @return map with values
     */
    public Map<String, String> asMap(){
        final Map<String, String> map = new HashMap<>();
        forEachRemaining( sse -> map.put(sse.getKey(), sse.getValue()));
        return Collections.unmodifiableMap(map);
    }


}
