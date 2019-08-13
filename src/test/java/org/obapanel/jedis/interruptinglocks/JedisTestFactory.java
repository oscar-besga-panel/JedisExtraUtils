package org.obapanel.jedis.interruptinglocks;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisTestFactory {


    public static final String HOST = "127.0.0.1";
    public static final int PORT = 32770;
    public static final String URI = "redis://" + HOST + ":" + PORT;


    static Jedis createJedisClient(){
        HostAndPort hostAndPort = new HostAndPort(HOST,PORT);
        Jedis jedis = new Jedis(hostAndPort);

        return jedis;
    }

    static JedisPool createJedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(0);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT);
        return jedisPool;
    }


}
