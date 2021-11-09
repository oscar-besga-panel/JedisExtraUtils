@Grab(group='redis.clients', module='jedis', version='2.10.2')
@Grab(group='org.obapanel.jedis', module='interruptinglocks', version='2.6.0')

import org.obapanel.jedis.iterators.AbstractScanIterator
import org.obapanel.jedis.iterators.HScanIterator
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.Protocol

import java.time.Duration

host = "localhost"
port = 6379
pass = ""

JedisPool jedisPool = null;
JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
jedisPoolConfig.setMaxTotal(24);
jedisPoolConfig.setMaxIdle(24);
jedisPoolConfig.setMinIdle(4);
// High performance
//        jedisPoolConfig.setMaxTotal(128);
//        jedisPoolConfig.setMaxIdle(128);
//        jedisPoolConfig.setMinIdle(16);
jedisPoolConfig.setTestOnBorrow(true);
jedisPoolConfig.setTestOnReturn(true);
jedisPoolConfig.setTestWhileIdle(true);
jedisPoolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(30).toMillis());
jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(10).toMillis());
jedisPoolConfig.setNumTestsPerEvictionRun(1);
jedisPoolConfig.setBlockWhenExhausted(true);
if (pass != null && !pass.trim().isEmpty()) {
    jedisPool = new JedisPool(jedisPoolConfig, host, port, Protocol.DEFAULT_TIMEOUT, pass);
} else {
    jedisPool = new JedisPool(jedisPoolConfig, host, port);
}

def getRedisType(JedisPool jedisPool1, String key1) {
    String result1 = ""
    Jedis jedis1 = null
    try {
        jedis1 = jedisPool1.getResource()
        result1 = jedis1.type(key1)
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        if (jedis1 != null) {
            jedis1.close();
        }
    }
    result1
}

ScanIterator scanIterator = new ScanIterator(jedisPool);
for (String rkey : scanIterator) {
    String rtype = getRedisType(jedisPool, rkey)
    println "${rkey} ${rtype}"
}
