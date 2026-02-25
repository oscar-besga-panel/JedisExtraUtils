package org.oba.jedis.extra.utils.utils;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.providers.ConnectionProvider;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JedisPoolToUnifiedRedis {

    private static Logger LOGGER = LoggerFactory.getLogger(JedisPoolToUnifiedRedis.class);


    public static UnifiedJedis asUnifiedJedis(JedisPool jedisPool) {
        ConnectionProviderForUnifiedRedis connectionProviderForUnifiedRedis = new ConnectionProviderForUnifiedRedis(jedisPool);
        UnifiedJedis unifiedJedis = new UnifiedJedis(connectionProviderForUnifiedRedis);
        return unifiedJedis;
    }

    private static class ConnectionProviderForUnifiedRedis implements ConnectionProvider {

        private final JedisPool jedisPool;
        private final boolean closeJedisPoolOnClosingUnifiedJedis;

        ConnectionProviderForUnifiedRedis(JedisPool jedisPool) {
            this(jedisPool, false);
        }

        ConnectionProviderForUnifiedRedis(JedisPool jedisPool, boolean closeJedisPoolOnClosingUnifiedJedis) {
            this.jedisPool = jedisPool;
            this.closeJedisPoolOnClosingUnifiedJedis = closeJedisPoolOnClosingUnifiedJedis;
        }

        @Override
        public Connection getConnection(CommandArguments args) {
            return createConnectionProxy();
        }


        @Override
        public Connection getConnection() {
            return createConnectionProxy();
        }

        @Override
        public void close() throws Exception {
            if (closeJedisPoolOnClosingUnifiedJedis) {
                jedisPool.close();
            }
        }

        // https://stackoverflow.com/questions/3291637/alternatives-to-java-lang-reflect-proxy-for-creating-proxies-of-abstract-classes
        Connection createConnectionProxy() {
            try {
                LOGGER.debug("Creating connection proxy from jedisPool");
                Jedis jedis = jedisPool.getResource();
                Connection connection =  jedis.getConnection();
                ProxyFactory factory = new ProxyFactory();
                factory.setSuperclass(Connection.class);
                MethodHandler handler = new ConnectionProxyMethodHandler(jedis, connection);
                Connection connectionProxy = (Connection)factory.create(new Class<?>[0], new Object[0], handler);
                return connectionProxy;
            } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
                     InvocationTargetException e) {
                LOGGER.error("Error while creating connection proxy", e);
                throw new IllegalStateException("Error while creating connection proxy", e);
            }
        }

    }

    public static class ConnectionProxyMethodHandler implements MethodHandler {

        private final Jedis jedis;
        private final Connection connection;

        public ConnectionProxyMethodHandler(Jedis jedis, Connection connection) {
            this.jedis = jedis;
            this.connection = connection;
        }


        @Override
        public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
            if (thisMethod.getName().equals("toString")) {
                LOGGER.debug("Invoke -> toString");
                return connection.toString();
            } else if (thisMethod.getName().equals("close")) {
                LOGGER.debug("Invoke -> close");
                jedis.close();
                //connection.close(); // ??
                return null;
            } else {
                LOGGER.debug("invoke call -> Object self {}, Method thisMethod {}, Method proceed {}, Object[] args {}",
                        self, thisMethod, proceed, args);
                return thisMethod.invoke(connection, args);
            }
        }

    }

}
