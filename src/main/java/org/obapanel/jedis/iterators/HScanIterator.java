package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Map;

/**
 * Iterator scan for the keys of a hmap
 * Only one use for an instace of this class
 *
 * Jedis pool connection is required
 * Name of the element on redis is required
 * (if not exists, it acts like as called for an empty map)
 *
 * If no pattern is provided, all elements are retrieves interactively
 * If no results per call to redis, it tries with 1
 *
 * Can return duplicated results, but is rare
 */
public class HScanIterator extends AbstractScanIterator<Map.Entry<String, String>> {


    private static final Logger LOGGER = LoggerFactory.getLogger(HScanIterator.class);

    private final String name;
    private final ScanParams scanParams;

    /**
     * Iterator for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     */
    public HScanIterator(JedisPool jedisPool, String name) {
        this(jedisPool, name, null, 1);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     */
    public HScanIterator(JedisPool jedisPool, String name, String pattern) {
        this(jedisPool, name, pattern, 1);
    }

    /**
     * Iterator for hmap entries
     * @param jedisPool Jedis connection pool
     * @param name Name of the hmap
     * @param pattern Pattern to be matched on the responses
     * @param resultsPerScan results per call to redis
     */
    public HScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
        super(jedisPool);
        this.name = name;
        this.scanParams = generateNewScanParams(pattern, resultsPerScan);
    }

    @Override
    ScanParams getScanParams() {
        return scanParams;
    }

    @Override
    ScanResult<Map.Entry<String, String>> doScan(Jedis jedis, String currentCursor, ScanParams scanParams) {
        return jedis.hscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(Jedis jedis, Map.Entry<String, String> next) {
        jedis.hdel(name, next.getKey());
    }


//    private final JedisPool jedisPool;
//    private final String name;
//
//    private final ScanParams scanParams;
//    private final Queue<Map.Entry<String, String>> nextValues = new LinkedList<>();
//    private ScanResult<Map.Entry<String, String>> currentResult;
//    private Map.Entry<String, String> next;
//
//    public HScanIterator(JedisPool jedisPool, String name) {
//        this(jedisPool, name, "", 1);
//    }
//
//    public HScanIterator(JedisPool jedisPool, String name, String pattern) {
//        this(jedisPool, name, pattern, 1);
//    }
//
//    public HScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
//        this.jedisPool = jedisPool;
//        this.name = name;
//        scanParams = new ScanParams().match(pattern).count(resultsPerScan);
//    }
//
//    @Override
//    public Map.Entry<String, String> next() {
//        next = nextValues.poll();
//        LOGGER.debug("Data next {} ", next);
//        return next;
//    }
//
//    @Override
//    public boolean hasNext() {
//        if (nextValues.isEmpty()) {
//            nextValues.addAll(scanForMoreValues());
//        }
//        return !nextValues.isEmpty();
//    }
//
//    private List<Map.Entry<String, String>> scanForMoreValues() {
//        if (currentResult == null || !currentResult.isCompleteIteration()) {
//            String currentCursor;
//            if (currentResult == null) {
//                currentCursor = ScanParams.SCAN_POINTER_START;
//            } else {
//                currentCursor = currentResult.getCursor();
//            }
//            LOGGER.debug("Petition with currentCursor " + currentCursor);
//            try (Jedis jedis = jedisPool.getResource()) {
//                currentResult = jedis.hscan(name, currentCursor, scanParams);
//            }
//            LOGGER.debug("Recovered data list is {}  with cursor {} ", currentResult.getResult(), currentResult.getCursor());
//            return currentResult.getResult();
//        } else {
//            LOGGER.debug("Recovered data list is EMPTY  with complete current iteration ");
//            return Collections.emptyList();
//        }
//    }
//
//
//    public void remove() {
//        if (next != null) {
//            try (Jedis jedis = jedisPool.getResource()) {
//                jedis.hdel(name, next.getKey());
//                next = null;
//            }
//        } else {
//            throw new IllegalStateException("Next not called or other error");
//        }
//    }
}
