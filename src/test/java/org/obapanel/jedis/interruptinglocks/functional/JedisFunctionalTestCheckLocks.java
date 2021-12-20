package org.obapanel.jedis.interruptinglocks.functional;

import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.interruptinglocks.IJedisLock;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.interruptinglocks.Lock;
import org.obapanel.jedis.utils.JedisPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisFunctionalTestCheckLocks {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisFunctionalTestCheckLocks.class);


    public static void main(String[] args) {
        JedisTestFactory jtfTest = JedisTestFactory.get();
        Jedis jedis = jtfTest.createJedisClient();
        jtfTest.testConnection();
        JedisPoolAdapter jedisPoolAdapter = JedisPoolAdapter.poolFromJedis(jedis);
        Lock jedisLock = new JedisLock(jedisPoolAdapter,"jedisLockSc").asConcurrentLock();
        boolean locked = jedisLock.tryLock();
        boolean reallyLocked = jedisLock.isLocked();
        jedisLock.unlock();
        jedisPoolAdapter.close();
        jedis.close();
        System.out.println("JEDISLOCK " + locked + " " + reallyLocked);

        jtfTest.testPoolConnection();
        JedisPool jedisPool = jtfTest.createJedisPool();
        Lock jedisPoolLock = new JedisLock(jedisPool,"jedisPoolLock").asConcurrentLock();
        boolean plocked = jedisPoolLock.tryLock();
        boolean preallyLocked = jedisPoolLock.isLocked();
        jedisPoolLock.unlock();
        jedisPool.close();
        System.out.println("JEDISPOOLLOCK " + plocked + " " + preallyLocked);

    }


}
