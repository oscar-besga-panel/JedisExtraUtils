package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * Iterator scan for the keys of the redis database
 */
public final class ScanIterator extends AbstractScanIterator<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanIterator.class);

    private final ScanParams scanParams;

    public ScanIterator(JedisPool jedisPool) {
        this(jedisPool, "", 1);
    }

    public ScanIterator(JedisPool jedisPool, String pattern) {
        this(jedisPool, pattern, 1);
    }


    public ScanIterator(JedisPool jedisPool, String pattern, int resultsPerScan) {
        super(jedisPool);
        this.scanParams = new ScanParams().match(pattern).count(resultsPerScan);
    }

    @Override
    ScanParams getScanParams() {
        return scanParams;
    }

    @Override
    ScanResult<String> doScan(Jedis jedis, String currentCursor, ScanParams scanParams) {
        return jedis.scan(currentCursor, scanParams);
    }

    @Override
    void doRemove(Jedis jedis, String next) {
        jedis.del(next);
    }
}
