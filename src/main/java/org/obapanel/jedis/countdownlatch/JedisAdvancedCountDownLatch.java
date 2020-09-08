package org.obapanel.jedis.countdownlatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This CountDownLatch works, but it is deprecated because its usage is very tricky
 * The await method waits por a pub/sub message that is sent when counter reaches zero
 * But waiting for a message forces the code to do some strange decisions
 * This is because
 * - We must use a JedisPubSub to listen to the message on the channel
 * - The connection that is listening with the JedisPubSub can't be used to other operations while waiting
 *   (yes after waiting and after message is sent)
 * - There's no natural way to interrupt the waiting programatically, the connection must be closed.
 *   For this cause, there is a interrupting method.
 *   And it is no easy way to code an await method with a timeout
 *
 * For this cause, I prefer the JedisCountDownLatch implementation with polling.
 * But this is here for anyone to use or refactor
 */
@Deprecated
public class JedisAdvancedCountDownLatch {

    private static final Logger LOG = LoggerFactory.getLogger(JedisAdvancedCountDownLatch.class);

    private static final String JEDIS_COUNTDOWNLATCH_CHANNEL_PREFIX = "JedisCountDownLatchChannel:";
    private static final String ZERO = "0";
    private static final String SOCKET_CLOSED = "java.net.SocketException: Socket is closed";
    private static final Long LONG_NULL_VALUE = -1L;

    public static final String COUNTDOWNLATCH_LUA_SCRIPT = "" +
            "local latch = redis.call('decr', KEYS[1]);" + "\n" +
            "if (latch <= 0) then " + "\n" +
            "    redis.call('publish', KEYS[2], ARGV[1]);" + "\n" +
            "end" + "\n" +
            "return latch;";


    private final Jedis jedis;
    private final String name;
    private final String channelName;
    private JedisPubSub jedisPubSub;
    private AtomicBoolean waiting = new AtomicBoolean(false);
    private AtomicBoolean interruptedSockedManually = new AtomicBoolean(false);

    public JedisAdvancedCountDownLatch(Jedis jedis, String name) {
        this(jedis, name, 1);
    }

    public JedisAdvancedCountDownLatch(Jedis jedis, String name, long count) {
        this.jedis = jedis;
        this.name = name;
        this.channelName = JEDIS_COUNTDOWNLATCH_CHANNEL_PREFIX + name;
        init(count);
    }

    private void init(long count){
        if (count <= 0) {
            throw new IllegalArgumentException("initial count on countdownlatch must be always more than zero");
        }
        jedis.set(name, String.valueOf(count), new SetParams().nx());
    }

    public void await() {
        if (getCount() > 0) {
            try {
                waiting.set(true);
                jedisPubSub = new CountDownLatchPubSub();
                LOG.info("await go subscribe");
                jedis.subscribe(jedisPubSub, channelName);
            } catch (JedisConnectionException jce){
                //LOG.error("Exception ", jce);
                checkIfInterruptedManually(jce);
            } finally {
                waiting.set(false);
            }
            LOG.debug("await ended");
        }
    }

    void checkIfInterruptedManually(JedisConnectionException jce) {
        if (SOCKET_CLOSED.equalsIgnoreCase(jce.getMessage()) &&
                jce.getCause() instanceof java.net.SocketException &&
                interruptedSockedManually.get() ) {
            jedisPubSub.unsubscribe(channelName);
            LOG.debug("await interrupted manually");
        } else {
            throw jce;
        }
    }

    private void onMessageRecieved(String channel, String message) {
        if (channelName.equalsIgnoreCase(channel) &&  ZERO.equalsIgnoreCase(message)){
            jedisPubSub.unsubscribe(channelName);
        }
    }

    public void countDown() {
        if (waiting.get()){
            throw new IllegalStateException("CountDownLatch is already waiting, no other operations allowed");
        }
        Object oresult = jedis.eval(COUNTDOWNLATCH_LUA_SCRIPT, Arrays.asList(name, channelName), Arrays.asList(ZERO));
        LOG.info("oresult {}", oresult);
    }

    private void countDownWithoutScript() {
        if (waiting.get()){
            throw new IllegalStateException("CountDownLatch is already waiting, no other operations allowed");
        }
        jedis.decr(name);
        if (getCount() == 0) {
            jedis.publish(channelName,ZERO);
        }
    }

    public void countDownAndWait() throws InterruptedException {
        countDown();
        await();
    }

    public long getCount() {
        if (waiting.get()){
            throw new IllegalStateException("CountDownLatch is already waiting, no other operations allowed");
        }
        String value = jedis.get(name);
        if (value != null && !value.isEmpty()) {
            return Long.parseLong(value);
        } else {
            return LONG_NULL_VALUE;
        }
    }

    public void interruptWaiting() {
        try {
            if (waiting.get()) {
                interruptedSockedManually.set(true);
                jedis.getClient().getSocket().close();
                jedis.close();
            }
        } catch (IOException ioe){
            LOG.error("Error while interruptWaiting on CountDownLatch {} ", name, ioe);
        }
    }

    private class CountDownLatchPubSub extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            LOG.info("CountDownLatchPubSub channel {} message {}", channel, message);
            onMessageRecieved(channel, message);
        }
    }

    /**
     * CAUTION !!
     * THIS METHOD DELETES THE REMOTE VALUE DESTROYING THIS COUNTDOWNLATCH AND OHTERS
     * USE AT YOUR OWN RISK WHEN ALL POSSIBLE OPERATIONS ARE FINISHED
     */
    public void destroy(){
        jedis.del(name);
    }

}
