package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

/**
 * Iterator scan for the keys of the redis database
 * It is abstract so it can be applied to SCAN, HSCAN, SSCAN
 */
abstract class AbstractScanIterator<K> implements Iterator<K> {


    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScanIterator.class);


    private final JedisPool jedisPool;

    private final Queue<K> nextValues = new LinkedList<>();
    private ScanResult<K> currentResult;
    private K next;

    /**
     * Base constrcutor
     * @param jedisPool Jedis pool to be used
     */
    public AbstractScanIterator(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    /**
     * Retrieve scan params to use in scan
     * @return scan params to be used
     */
    abstract ScanParams getScanParams();

    /**
     * Generates a new ScanParams object
     * @param pattern Patter to use, can be null or empty (ignored in that case)
     * @param resultsPerScan (ignored if 0 or less)
     * @return Newver null ScanParams object
     */
    public static ScanParams generateNewScanParams(String pattern, int resultsPerScan ) {
        if ((pattern == null || pattern.isEmpty()) && resultsPerScan <= 0) {
            return new ScanParams();
        } else if (pattern == null || pattern.isEmpty()) {
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
            try (Jedis jedis = jedisPool.getResource()) {
                currentResult = doScan(jedis, currentCursor, getScanParams());
            }
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
            try (Jedis jedis = jedisPool.getResource()) {
                //jedis.del(next);
                doRemove(jedis, next);
                next = null;
            }
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


}
