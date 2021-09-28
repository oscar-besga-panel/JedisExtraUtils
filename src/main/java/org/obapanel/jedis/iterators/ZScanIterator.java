package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

public class ZScanIterator extends AbstractScanIterator<Tuple>  {


    private static final Logger LOGGER = LoggerFactory.getLogger(ZScanIterator.class);

    private final String name;
    private final ScanParams scanParams;


    public ZScanIterator(JedisPool jedisPool, String name) {
        this(jedisPool, name, null, 1);
    }

    public ZScanIterator(JedisPool jedisPool, String name, String pattern) {
        this(jedisPool, name, pattern, 1);
    }

    public ZScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
        super(jedisPool);
        this.name = name;
        this.scanParams = generateNewScanParams(pattern, resultsPerScan);
    }


    @Override
    ScanParams getScanParams() {
        return scanParams;
    }

    @Override
    ScanResult<Tuple> doScan(Jedis jedis, String currentCursor, ScanParams scanParams) {
        return jedis.zscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(Jedis jedis, Tuple next) {
        jedis.zrem(name, next.getElement());
    }


}
