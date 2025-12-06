package org.oba.jedis.extra.utils.test;

import org.oba.jedis.extra.utils.iterators.ScanIterable;
import org.oba.jedis.extra.utils.iterators.ScanIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class JedisTestFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(JedisTestFactory.class);

    private static final AtomicInteger numClient = new AtomicInteger(0);

    private static final String ABC = "abcdefhijklmnopqrstuvwxyz";

    public static final String TEST_PROPERTIES_FILE = "jedis.test.properties";
    public static final String TEST_PROPERTIES_PATH = "./src/test/resources/" + TEST_PROPERTIES_FILE;

    private static final String PREFIX = "jedis.test.";

    private static final int DEFAULT_FUNCTIONAL_TEST_CYCLES = 0;
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 6379;
    private static final String DEFAULT_USER = "";
    private static final String DEFAULT_PASS = "";
    private static final boolean DEFAULT_ENABLE_SENTINEL = false;
    private static final String DEFAULT_SENTINEL_HOSTS = "";
    private static final String DEFAULT_SENTINEL_MASTER = "";
    private static final String DEFAULT_SEMTIMEL_PASS = "";

    public static JedisTestFactory get() {
        return JedisTestFactoryHolder.instance;
    }

    private static class JedisTestFactoryHolder {
        static JedisTestFactory instance = new JedisTestFactory();
    }

    private int functionalTestCycles = DEFAULT_FUNCTIONAL_TEST_CYCLES;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String user = DEFAULT_USER;
    private String pass = DEFAULT_PASS;
    private boolean enableSentinel = DEFAULT_ENABLE_SENTINEL;
    private String sentinelHosts = DEFAULT_SENTINEL_HOSTS;
    private String sentinelMaster = DEFAULT_SENTINEL_MASTER;
    private String sentinelPass = DEFAULT_SEMTIMEL_PASS;


    private boolean testConnectionOk = true;

    public JedisTestFactory() {
        load();
    }

    private void load() {
        Properties properties = readFromFile();
        if (properties == null) {
            properties = readFromClasspath();
        }
        if (properties != null) {
            assignProperties(properties);
        }
        try {
            if (functionalTestEnabled()) {
                testConnection();
            }
        } catch (Exception e) {
            testConnectionOk = false;
            LOGGER.error("ERROR testing jedis connnections ",e);
        }
    }

    private Properties readFromFile() {
        return readFrom("file", () -> new FileInputStream(TEST_PROPERTIES_PATH));
    }

    private Properties readFromClasspath() {
        return readFrom("classpath", () -> JedisTestFactory.class.getClassLoader().getResourceAsStream(TEST_PROPERTIES_FILE));
    }

    private Properties readFrom(String fromWhat, IoSupplier inputStreamSupplier) {
        try (InputStream input = inputStreamSupplier.get()) {
            if (input == null) {
                throw new IllegalStateException("Input is null, not loaded from " + fromWhat);
            }
            Properties properties = new Properties();
            properties.load(input);
            LOGGER.debug("Ok loading from {} ", fromWhat);
            return properties;
        } catch (Exception e) {
            LOGGER.debug("Error loading from {} ", fromWhat, e);
            return null;
        }
    }

    private void assignProperties(Properties properties) {
        functionalTestCycles = Integer.parseInt(properties.getProperty(PREFIX + "functionalTestCycles",
                String.valueOf(DEFAULT_FUNCTIONAL_TEST_CYCLES)));
        host = properties.getProperty(PREFIX + "host", DEFAULT_HOST);
        port = Integer.parseInt(properties.getProperty(PREFIX + "port", String.valueOf(DEFAULT_PORT)));
        user = properties.getProperty(PREFIX + "user", DEFAULT_USER);
        pass = properties.getProperty(PREFIX + "pass", DEFAULT_PASS);
        enableSentinel = "true".equalsIgnoreCase(properties.getProperty(PREFIX + "enableSentinel",
                String.valueOf(DEFAULT_ENABLE_SENTINEL)));
        sentinelHosts = properties.getProperty(PREFIX + "sentinel.hosts", DEFAULT_SENTINEL_HOSTS);
        sentinelMaster = properties.getProperty(PREFIX + "sentinel.master", DEFAULT_SENTINEL_MASTER);
        sentinelPass = properties.getProperty(PREFIX + "sentinel.pass", DEFAULT_SEMTIMEL_PASS);
    }

    public boolean functionalTestEnabled(){
        return testConnectionOk && functionalTestCycles > 0;
    }

    public int getFunctionalTestCycles() {
        return functionalTestCycles;
    }


    public JedisPooled createJedisPooled() {
        if (enableSentinel) {
            return createJedisPooledSentinel();
        } else {
            return createJedisPooledClassic();
        }
    }

    public JedisPooled createJedisPooled(int maxConns, int minIdleConns) {
        return createJedisPooledClassic(maxConns, minIdleConns);
    }

    public JedisPooled createJedisPooledClassic() {
        return createJedisPooledClassic(7,3);
    }

    public JedisPooled createJedisPooledClassic(int maxConns, int minIdleConns) {
        DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder();
        if (user != null && !user.trim().isEmpty()) {
            configBuilder.user(user);
        }
        if (pass != null && !pass.trim().isEmpty()) {
            configBuilder.password(pass);
        }
        configBuilder.clientName( String.join("_", "JedisTestFactory",
                Integer.toString(numClient.incrementAndGet()), Long.toString(System.currentTimeMillis())));
        configBuilder.database(0).timeoutMillis(120000);
        JedisClientConfig config = configBuilder.build();
        HostAndPort address = new HostAndPort(host, port);
        ConnectionPoolConfig poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxTotal(maxConns);
        poolConfig.setMaxWait(Duration.ofSeconds(30));
        poolConfig.setTestOnReturn(true);
        poolConfig.setMinIdle(minIdleConns);
        return new JedisPooled(poolConfig, address, config);
    }


    public JedisPooled createJedisPooledSentinel() {
        throw new UnsupportedOperationException("no sentinel available for JedisPooled");
    }

    public void testConnection() {
        try (JedisPooled jedisPooled = createJedisPooled()){
            testConnection(jedisPooled);
        }
    }

    public void testConnection(JedisPooled jedisPooled){
        String val = "test:" + System.currentTimeMillis();
        jedisPooled.set(val,val,new SetParams().px(5000));
        String check = jedisPooled.get(val);
        jedisPooled.del(val);
        if (!val.equalsIgnoreCase(check))
            throw new IllegalStateException("Jedis connection not ok");
        if (!jedisPooled.ping().equalsIgnoreCase("PONG"))
            throw new IllegalStateException("Jedis connection not pong");
    }

    public List<String> randomSizedListOfChars() {
        int size = ThreadLocalRandom.current().nextInt(5, ABC.length());
        List<String> result = new ArrayList<>(size);
        for(int i=0; i < size ; i++) {
            result.add(String.valueOf(ABC.toCharArray()[i]));
        }
        return result;
    }

    interface IoSupplier {
        InputStream get() throws IOException;
    }

    /**
     * Main method
     * Run only if avalible connnection on jedis.test.properties file
     * @param args arguments
     */
    public static void main(String[] args) {
        LOGGER.debug("main ini >>>> ");
        JedisTestFactory jedisTestFactory = JedisTestFactory.get();
        jedisTestFactory.testConnection();
        JedisPooled jedisPooled = jedisTestFactory.createJedisPooled(15,5);
        ScanIterable scanIterable = new ScanIterable(jedisPooled);
        scanIterable.forEach(rkey -> LOGGER.debug("KEY: {} - TYPE {} ", rkey, jedisPooled.type(rkey)));
        LOGGER.debug("main fin <<<< ");
    }

}
