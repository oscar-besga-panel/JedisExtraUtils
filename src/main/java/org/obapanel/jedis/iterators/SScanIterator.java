package org.obapanel.jedis.iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class SScanIterator extends AbstractScanIterator<String>  {


    private static final Logger LOGGER = LoggerFactory.getLogger(SScanIterator.class);

    private final String name;
    private final ScanParams scanParams;


    public SScanIterator(JedisPool jedisPool, String name) {
        this(jedisPool, name, null, 1);
    }

    public SScanIterator(JedisPool jedisPool, String name, String pattern) {
        this(jedisPool, name, pattern, 1);
    }

    public SScanIterator(JedisPool jedisPool, String name, String pattern, int resultsPerScan) {
        super(jedisPool);
        this.name = name;
        this.scanParams = generateNewScanParams(pattern, resultsPerScan);
    }



    @Override
    ScanParams getScanParams() {
        return scanParams;
    }

    @Override
    ScanResult<String> doScan(Jedis jedis, String currentCursor, ScanParams scanParams) {
        return jedis.sscan(name, currentCursor, scanParams);
    }

    @Override
    void doRemove(Jedis jedis, String next) {
        jedis.srem(name, next);
    }


}
