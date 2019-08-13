package org.obapanel.jedis.interruptinglocks;

import redis.clients.jedis.Jedis;

public class MainCheckLock {

    public static void main(String[] args) {
        Jedis jedis = JedisTestFactory.createJedisClient();
        jedis.set("x",System.currentTimeMillis()+"");
        Lock jedisLock = new JedisLock(jedis,"jedisLock").asConcurrentLock();
        boolean locked = jedisLock.tryLock();
        jedisLock.unlock();
        jedis.quit();
        System.out.println("JEDISLOCK");

    }
}
