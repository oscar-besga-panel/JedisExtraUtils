package org.oba.jedis.extra.utils.utils;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class creates a new 'pool' of redis connections
 * from only one existing connection.
 *
 * This fake pool will return the same connection again
 * and again
 *
 * In reality it will return a proxy that will execute
 * all the opetations except close.
 * Close operation on the returned jedis connection will do nothing
 * This is because a close on a pool connection will not close it but retrun to
 * the pool.
 * And I do not like to cut connection when closed when it comes from pool, from example
 * in a try-with-resources
 *
 * Closing the pool will nullify the connection inside the pool making it unusable but
 * it will not change the original connection
 *
 * This class is not, in any way, thread safe.
 * Use with caution
 */
public class JedisPoolAdapter extends JedisPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolAdapter.class);

    private static final String CLOSE = "close";

    private final Jedis jedis;
    private final AtomicBoolean isClosedStatus = new AtomicBoolean(false);

    /**
     * Creates a pool from a single connection
     * @param jedis connection to redis
     * @return pool of connection
     */
    public static JedisPoolAdapter poolFromJedis(Jedis jedis) {
        return new JedisPoolAdapter(jedis);
    }

    /**
     * Creates a JedisPoolAdapter from a single connection
     * @param jedis Jedis connection
     */
    public JedisPoolAdapter(Jedis jedis) {
        if (jedis == null) {
            throw new IllegalArgumentException("Jedis should not be null");
        }
        this.jedis = jedis;
    }

    /**
     * Execute the consumer with the resource
     * @param action Consumer of redis connection
     */
    public void withResource(Consumer<Jedis> action) {
        try(Jedis jedis = getResource()) {
            action.accept(jedis);
        }
    }

    /**
     * Execute the consumer with the resource
     * and gets the result
     * @param action Function of redis connection
     * @return result of function
     */
    public <T> T withResourceFunction(Function<Jedis, T> action) {
        try(Jedis jedis = getResource()) {
            return action.apply(jedis);
        }
    }


    @Override
    public Jedis getResource() {
        if (isClosedStatus.get()) {
            throw new IllegalStateException("JedisPoolAdapter already closed");
        }
        return createDynamicProxyFromJedis(jedis);
    }

    @Override
    public void returnBrokenResource(Jedis resource) {
        // NOPE
    }

    @Override
    public void returnResource(Jedis resource) {
        // NOPE
    }

    @Override
    public void close() {
        isClosedStatus.set(true);
        super.close();
    }

    @Override
    public int getNumActive() {
        return 1;
    }

    @Override
    public int getNumIdle() {
        return 0;
    }

    @Override
    public int getNumWaiters() {
        return 0;
    }

    @Override
    public void addObjects(int count) {
        //NOPE
    }


    /**
     * This will create a proxy object
     * The intent is to have a proxy that will execute every method except close()
     *
     * Because close() can be called manually by the developer or automatically by the try-with-resources
     *   and this close() will be expected to return the connection to the pool
     *   not to close definitely the connection to redis
     *   (and this pool will be effectively closed)
     *   the proxy will capture the close method of the resource-returned jedis connection
     *   and do nothing
     *   and avoid errors.
     *
     *
     * @param jedis jedis connection
     * @return jedis proxied connection
     */
    private static Jedis createDynamicProxyFromJedis(final Jedis jedis)  {
        try {
            ProxyFactory factory = new ProxyFactory();
            factory.setSuperclass(Jedis.class);
            MethodHandler handler = (self, method, proceed, args) -> {
                LOGGER.debug("Handling " + method + " via the method handler");
                if (CLOSE.equals(method.getName())) {
                    LOGGER.debug("Avoid close by proxy");
                    return null;
                } else {
                    return method.invoke(jedis, args);
                }
            };
            return (Jedis) factory.create(new Class<?>[0], new Object[0], handler);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Error in jedis dynamic proxy", e);
        }
    }

}
