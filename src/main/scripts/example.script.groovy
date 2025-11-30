/**
 * Example of groovy script
 */
/*
@Grab(group='redis.clients', module='jedis', version='7.1.0')
@Grab(group='org.obapanel.jedis', module='interruptinglocks', version='7.1.0')
*/

import org.oba.jedis.extra.utils.iterators.ScanIterable
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;
import org.oba.jedis.extra.utils.iterators.ScanIterator


host = "localhost"
port = 6379
user = ""
pass = ""


DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder()
if (user != null && !user.trim().isEmpty()) {
    configBuilder.user(user)
}
if (pass != null && !pass.trim().isEmpty()) {
    configBuilder.password(pass)
}
configBuilder.clientName( String.join("_", "JedisTestFactory",
        "Groovy", Long.toString(System.currentTimeMillis())));
configBuilder.database(0).timeoutMillis(120000)
JedisClientConfig config = configBuilder.build()
HostAndPort address = new HostAndPort(host, port)
ConnectionPoolConfig poolConfig = new ConnectionPoolConfig()
poolConfig.setMaxTotal(5)
poolConfig.setTestOnReturn(true)
poolConfig.setMinIdle(1)
JedisPooled jedisPooled = new JedisPooled(poolConfig, address, config)

ScanIterable scanIterable = new ScanIterable(jedisPooled);
scanIterable.forEach(rkey -> println "KEY ${rkey} - VALUE ${jedisPooled.type(rkey)}");

