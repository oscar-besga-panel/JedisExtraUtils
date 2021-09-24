package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

public class SetScanIterator implements Iterator<String>  {


    private static final Logger LOGGER = LoggerFactory.getLogger(SetScanIterator.class);


    private final JedisPool jedisPool;
    private final String name;

    private final ScanParams scanParams;
    private final Queue<String> nextValues = new LinkedList<>();
    private ScanResult<String> currentResult;
    private String next;

    public SetScanIterator(JedisPool jedisPool, String name){
        this(jedisPool, name, "", 1);
    }

    public SetScanIterator(JedisPool jedisPool, String name, String pattern){
        this(jedisPool, name, pattern, 1);
    }

    public SetScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan){
        this.jedisPool = jedisPool;
        this.name = name;
        scanParams = new ScanParams().match(pattern).count(resultsPerScan);
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
            String currentCursor;
            if (currentResult == null) {
                currentCursor = ScanParams.SCAN_POINTER_START;
            } else {
                currentCursor = currentResult.getCursor();
            }
            LOGGER.debug("Petition with currentCursor " + currentCursor);
            try (Jedis jedis = jedisPool.getResource()) {
                currentResult = jedis.sscan(name, currentCursor, scanParams);
            }
            LOGGER.debug("Recovered data list is {}  with cursor {} ", currentResult.getResult(), currentResult.getCursor());
            return currentResult.getResult();
        }



        public void remove() {
            if (next != null) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.srem(name, next);
                    next = null;
                }
            } else {
                throw new IllegalStateException("Next not called or other error");
            }
        }
}
