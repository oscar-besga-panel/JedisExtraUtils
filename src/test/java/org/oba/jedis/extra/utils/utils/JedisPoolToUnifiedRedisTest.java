package org.oba.jedis.extra.utils.utils;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JedisPoolToUnifiedRedisTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisPoolToUnifiedRedisTest.class);

    @Mock
    JedisPool jedisPool;

    @Mock
    Jedis jedis;

    @Mock
    Connection connection;

    @Before
    public void setup() {
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedis.getConnection()).thenReturn(connection);
    }

    @Test
    public void asUnifiedJedisTest() {
        AtomicReference<String> launchedCommand = new AtomicReference<>("");
        when(connection.executeCommand(any(CommandObject.class))).thenAnswer( ioc -> {
            CommandObject commandObject =  ioc.getArgument(0, CommandObject.class);
            String tmp = new String(commandObject.getArguments().get(0).getRaw()).intern();
            launchedCommand.set(tmp);
            LOGGER.debug("executing command {} from commandObject {}", tmp, commandObject);
            return "PONG";
        });
        UnifiedJedis redisClient = JedisPoolToUnifiedRedis.asUnifiedJedis(jedisPool);
        String result = redisClient.ping();
        LOGGER.debug("result {}", result);
        verify(connection, times(1)).executeCommand(any(CommandObject.class));
        verify(jedis, times(2)).close();
        assertTrue("PING".equalsIgnoreCase(launchedCommand.get()));
        assertTrue(result != null && !result.isEmpty());
        assertTrue("PONG".equalsIgnoreCase(result));
    }

}
