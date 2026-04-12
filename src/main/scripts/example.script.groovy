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
import redis.clients.jedis.RedisClient


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

RedisClient redisClient = RedisClient.Builder().
        poolConfig(poolConfig).
        hostAndPort(host, port).
        build()

ScanIterable scanIterable = new ScanIterable(redisClient)
scanIterable.forEach(rkey -> println "KEY ${rkey} - VALUE ${redisClient.type(rkey)}");

