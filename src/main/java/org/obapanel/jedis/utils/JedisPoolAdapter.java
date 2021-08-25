package org.obapanel.jedis.utils;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisExhaustedPoolException;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Consumer;
import java.util.function.Function;

public class JedisPoolAdapter extends JedisPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolAdapter.class);

    private static final String CLOSE = "close";

    private Jedis jedis;

    public static JedisPoolAdapter poolFromJedis(Jedis jedis) {
        return new JedisPoolAdapter(jedis);
    }

    public JedisPoolAdapter(Jedis jedis) {
        this.jedis = jedis;
    }

    public void withResource(Consumer<Jedis> action) {
        try(Jedis jedis = getResource()) {
            action.accept(jedis);
        }
    }

    public <T> T withResourceFunction(Function<Jedis, T> action) {
        try(Jedis jedis = getResource()) {
            return action.apply(jedis);
        }
    }

    @Override
    public Jedis getResource() {
        if (jedis != null) {
            return createDynamicProxyFromJedis(jedis);
        } else {
            throw new JedisExhaustedPoolException("Jedis pool adapter is closed");
        }
    }

    @Override
    protected void returnBrokenResource(Jedis resource) {
        // NOPE
    }

    @Override
    protected void returnResource(Jedis resource) {
        // NOPE
    }

    @Override
    public void close() {
        jedis = null;
    }

    @Override
    public boolean isClosed() {
        return jedis == null || !jedis.isConnected();
    }

    @Override
    public void initPool(GenericObjectPoolConfig poolConfig, PooledObjectFactory<Jedis> factory) {
        // NOPE
    }

    @Override
    protected void returnResourceObject(Jedis resource) {
        // NOPE
    }

    @Override
    public void destroy() {
        close();
    }

    @Override
    protected void returnBrokenResourceObject(Jedis resource) {
        // NOPE
    }

    @Override
    protected void closeInternalPool() {
        // NOPE
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
    public long getMeanBorrowWaitTimeMillis() {
        return 0L;
    }

    @Override
    public long getMaxBorrowWaitTimeMillis() {
        return 0L;
    }

    @Override
    public void addObjects(int count) {
        //NOPE
    }


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
            throw new RuntimeException("Error in jedis dynamic proxy", e);
        }
    }

}
