package org.oba.jedis.extra.utils.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Connection;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.providers.ConnectionProvider;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

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
        public Connection getConnection() {
            Connection connection =  jedisPool.getResource().getConnection();
            Connection proxyInstance = (Connection) Proxy.newProxyInstance(JedisPoolToUnifiedRedis.class.getClassLoader(),
                    new Class[] { Connection.class },
                    new ConnectionInvocationHandler(connection));
            return proxyInstance;
        }

        @Override
        public Connection getConnection(CommandArguments args) {
            return this.getConnection();
        }

        @Override
        public void close() throws Exception {
            if (closeJedisPoolOnClosingUnifiedJedis) {
                jedisPool.close();
            }
        }

    }
    // https://www.baeldung.com/java-dynamic-proxies
    public static class ConnectionInvocationHandler implements InvocationHandler {

        private Connection connection;

        ConnectionInvocationHandler(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                LOGGER.warn("Closing a connection from JedisPool is not allowed. Please close the JedisPool instead.");
                return null;
            } else {
                return method.invoke(proxy, args);
            }
        }
    }

}
