package org.obapanel.jedis.interruptinglocks.functional;

import org.obapanel.jedis.common.test.JedisTestFactory;
import org.obapanel.jedis.utils.OnMessagePubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.atomic.AtomicReference;

public class JedisFunctionalTestSubscribePubSub {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisFunctionalTestSubscribePubSub.class);


    public static void sleeep(int seconds){
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void subscribe(JedisPool jedisPool, JedisPubSub jedisPubSub, String channel) {
        sleeep(2);
        try(Jedis jedis = jedisPool.getResource()) {
            System.out.printf("subscribe do subscribe channel %s%n", channel);
            jedis.subscribe(jedisPubSub, channel);
        }
        System.out.printf("subscribe do unsubscribe channel %s%n", channel);
    }


    public static void onMessageDo(String channel, String message, AtomicReference<JedisPubSub> jedisPubSub) {
        System.out.printf("onMessageDo channel %s message %s%n", channel, message);
        jedisPubSub.get().unsubscribe();
    }

    public static void publishMessage(JedisPool jedisPool, String channel, String message) {
        sleeep(4);
        try(Jedis jedis = jedisPool.getResource()) {
            System.out.printf("publishMessage do publish channel %s message %s%n", channel, message);
            jedis.publish(channel, message);
        }
        System.out.printf("publishMessage done publish channel %s message %s%n", channel, message);
    }


    public static void main(String[] args) {
        System.out.printf("main init%n");
        long t = System.currentTimeMillis();
        String channel = String.format("channel_%d", t);
        String message = String.format("message_%d", t);
        JedisTestFactory jtfTest = JedisTestFactory.get();
        JedisPool jedisPool = jtfTest.createJedisPool();
        jtfTest.testConnection();
        AtomicReference<JedisPubSub> jedisPubSubAtomicReference = new AtomicReference<>();
        OnMessagePubSub onMessagePubSub = new OnMessagePubSub((c,m) -> onMessageDo(c,m,jedisPubSubAtomicReference));
        jedisPubSubAtomicReference.set(onMessagePubSub);
        System.out.printf("main run threads");
        Thread subscribe = new Thread(() -> subscribe(jedisPool, onMessagePubSub, channel));
        subscribe.setName("subscribe");
        subscribe.setDaemon(true);
        subscribe.start();
        Thread publishMessage = new Thread(() -> publishMessage(jedisPool, channel, message));
        publishMessage.setName("publishMessage");
        publishMessage.setDaemon(true);
        publishMessage.start();
        System.out.printf("main wait%n");
        sleeep(1);
        try {
            subscribe.join();
            publishMessage.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.printf("main end%n");
        jedisPool.close();
    }


}
