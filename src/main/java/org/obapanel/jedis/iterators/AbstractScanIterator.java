package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;
import java.util.function.Consumer;

/**
 * Iterator scan for the keys of the redis database
 *
 */
public abstract class AbstractScanIterator<K> implements Iterator<K> {


    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractScanIterator.class);


    private final JedisPool jedisPool;

    private final ScanParams scanParams;
    private final Queue<K> nextValues = new LinkedList<>();
    private ScanResult<K> currentResult;
    private K next;

    public AbstractScanIterator(JedisPool jedisPool){
        this.jedisPool = jedisPool;
        this.scanParams = generateScanParams();
    }

    abstract ScanParams generateScanParams();



    @Override
    public K next() {
        next =  nextValues.poll();
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

        private List<K> scanForMoreValues() {
            String currentCursor;
            if (currentResult == null) {
                currentCursor = ScanParams.SCAN_POINTER_START;
            } else {
                currentCursor = currentResult.getCursor();
            }
            LOGGER.debug("Petition with currentCursor " + currentCursor);
            try (Jedis jedis = jedisPool.getResource()) {
                //currentResult = jedis.scan(currentCursor, scanParams);
                currentResult = doScan(jedis, currentCursor, scanParams);
            }
            LOGGER.debug("Recovered data list is {}  with cursor {} ", currentResult.getResult(), currentResult.getCursor());
            return currentResult.getResult();
        }

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

    abstract ScanResult<K> doRemove(Jedis jedis, K next);



}
