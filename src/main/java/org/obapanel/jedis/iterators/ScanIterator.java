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
public class ScanIterator implements Iterator<String> {


    private static final Logger LOGGER = LoggerFactory.getLogger(ScanIterator.class);


    private final JedisPool jedisPool;

    private final ScanParams scanParams;
    private final Queue<String> nextValues = new LinkedList<>();
    private ScanResult<String> currentResult;
    private String next;

    public ScanIterator(JedisPool jedisPool){
        this(jedisPool,"", 1);
    }

    public ScanIterator(JedisPool jedisPool, String pattern){
        this(jedisPool, pattern, 1);
    }

    public ScanIterator(JedisPool jedisPool, String pattern, int resultsPerScan){
        this(jedisPool, new ScanParams().match(pattern).count(resultsPerScan));
    }

    ScanIterator(JedisPool jedisPool, ScanParams scanParams){
        this.jedisPool = jedisPool;
        this.scanParams = scanParams;
    }

    @Override
    public String next() {
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

        private List<String> scanForMoreValues() {
            if (currentResult == null || !currentResult.isCompleteIteration()) {
                String currentCursor;
                if (currentResult == null) {
                    currentCursor = ScanParams.SCAN_POINTER_START;
                } else {
                    currentCursor = currentResult.getCursor();
                }
                LOGGER.debug("Petition with currentCursor " + currentCursor);
                try (Jedis jedis = jedisPool.getResource()) {
                    currentResult = jedis.scan(currentCursor, scanParams);
                }
                LOGGER.debug("Recovered data list is {}  with cursor {} ", currentResult.getResult(), currentResult.getCursor());
                return currentResult.getResult();
            } else {
                LOGGER.debug("Recovered data list is EMPTY  with complete current iteration ");
                return Collections.emptyList();
            }
        }

        public void remove() {
            if (next != null) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.del(next);
                    next = null;
                }
            } else {
                throw new IllegalStateException("Next not called or other error");
            }
        }
}
