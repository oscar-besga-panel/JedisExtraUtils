package org.oba.jedis.extra.utils.iterators;

import org.oba.jedis.extra.utils.utils.JedisPoolUser;
import org.oba.jedis.extra.utils.utils.Listable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;

/**
 * Iterator scan for the keys of the redis database
 * It is abstract so it can be applied to SCAN, HSCAN, SSCAN
 */
abstract class AbstractScanIterator<K> implements Iterator<K>, Listable<K>, JedisPoolUser {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScanIterator.class);


    public static final int DEFAULT_RESULTS_PER_SCAN_ITERATORS = 50;

    public static final String DEFAULT_PATTERN_ITERATORS = null;


    private final JedisPool jedisPool;
    private final ScanParams scanParams;

    private final Queue<K> nextValues = new LinkedList<>();
    private ScanResult<K> currentResult;
    private K next;

    /**
     * Base constrcutor
     * @param jedisPool Jedis pool to be used
     */
    AbstractScanIterator(JedisPool jedisPool, String pattern, int resultsPerScan) {
        this.jedisPool = jedisPool;
        this.scanParams = generateNewScanParams(pattern, resultsPerScan);
    }

    @Override
    public JedisPool getJedisPool() {
        return jedisPool;
    }



    /**
     * Retrieve scan params to use in scan
     * @return scan params to be used
     */
    public ScanParams getScanParams() {
        return scanParams;
    }

    /**
     * Generates a new ScanParams object
     * @param pattern Patter to use, can be null or empty (ignored in that case)
     * @param resultsPerScan (ignored if 0 or less)
     * @return Newver null ScanParams object
     */
    public static ScanParams generateNewScanParams(String pattern, int resultsPerScan ) {
        if ((pattern == null || pattern.trim().isEmpty()) && resultsPerScan <= 0) {
            return new ScanParams();
        } else if (pattern == null || pattern.trim().isEmpty()) {
            return new ScanParams().count(resultsPerScan);
        } else if (resultsPerScan <= 0) {
            return new ScanParams().match(pattern);
        } else {
            return new ScanParams().match(pattern).count(resultsPerScan);
        }
    }


    @Override
    public K next() {
        next = nextValues.poll();
        LOGGER.debug("Data next {} ", next);
        return next;
    }

    @Override
    public boolean hasNext() {
        if (nextValues.isEmpty()) {
            nextValues.addAll(scanForMoreValues());
        }
        return !nextValues.isEmpty();
    }

    /**
     * Gets the next values on the hasnext operation if needed
     * @return List of new values, can be empty
     */
    private List<K> scanForMoreValues() {
        if (currentResult == null || !currentResult.isCompleteIteration()) {
            String currentCursor;
            if (currentResult == null) {
                currentCursor = ScanParams.SCAN_POINTER_START;
            } else {
                currentCursor = currentResult.getCursor();
            }
            LOGGER.debug("Petition with currentCursor " + currentCursor);
            currentResult = withJedisPoolGet(jedis -> doScan(jedis, currentCursor, getScanParams()));
            LOGGER.debug("Recovered data list is {}  with cursor {} ", currentResult.getResult(), currentResult.getCursor());

            if (currentResult.getResult().isEmpty() && !currentResult.isCompleteIteration()) {
                LOGGER.debug("Recovered data list is empty but not exahusted, so we try another time (recursive) to not give an error");
                return scanForMoreValues(); // RECURSIVE!! IF RESULT EMPTY BUT MORE RESULTS CAN BE EXTRACTED
            } else {
                return currentResult.getResult();
            }
        } else {
            LOGGER.debug("Recovered data list is EMPTY  with complete current iteration ");
            return Collections.emptyList();
        }
    }

    /**
     * Implement this method to call jedis.scan / jedis.hscan / jedis.sscan / jedis.zscan
     * @param jedis Jedis connection from jedisPool, it will be closed after the call
     * @param currentCursor current cursor of call
     * @param scanParams scan params object
     * @return result of this call
     */
    abstract ScanResult<K> doScan(Jedis jedis, String currentCursor, ScanParams scanParams);

    public void remove() {
        if (next != null) {
            withJedisPoolDo(jedis -> doRemove(jedis, next));
            next = null;
        } else {
            throw new IllegalStateException("Next not called or other error");
        }
    }

    /**
     * Implement this method to call jedis.del / jedis.hrem / jedis.srem / jedis.zrem
     * @param jedis Jedis connection from jedisPool, it will be closed after the call
     * @param next data to be deleted
     */
    abstract void doRemove(Jedis jedis, K next);


    /**
     * This method returns ALL of the values of the iterator as a unmodificable list
     * This method consumes the iterator, no next and hasNetx method shoould be called
     * The list is unmodificable and contains no repeated elements
     * @return list with values
     */
    public List<K> asList(){
        final Set<K> set = new HashSet<>();
        forEachRemaining(set::add);
        return Collections.unmodifiableList(new ArrayList<>(set));
    }

}
