package org.obapanel.jedis.interruptinglocks.functional;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.obapanel.jedis.interruptinglocks.IJedisLock;
import org.obapanel.jedis.interruptinglocks.JedisLock;
import org.obapanel.jedis.utils.JedisPoolAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.obapanel.jedis.interruptinglocks.functional.JedisTestFactory.*;

public class FunctionalWritingFileScTest {

    private static final Logger log = LoggerFactory.getLogger(FunctionalWritingFileScTest.class);

    private final List<Jedis> jedisList = new ArrayList<>();
    private final List<JedisPool> jedisPoolList = new ArrayList<>();
    private String lockName;
    private final List<IJedisLock> lockList = new ArrayList<>();
    private final AtomicBoolean otherError = new AtomicBoolean(false);
    private int line = 0;


    @Rule
    public TemporaryFolder folder = new TemporaryFolder();



    @Before
    public void before() throws IOException {
        org.junit.Assume.assumeTrue(functionalTestEnabled());
        if (!functionalTestEnabled()) return;
        lockName = "lock:" + this.getClass().getName() + ":" + System.currentTimeMillis();
    }


    @After
    public void after() {
        jedisPoolList.forEach(JedisPool::close);
        jedisList.forEach(Jedis::close);
    }

    JedisPool createJedisPoolAdapter() {
        Jedis jedis = createJedisClient();
        jedisList.add(jedis);
        JedisPool jedisPool = JedisPoolAdapter.poolFromJedis(jedis);
        jedisPoolList.add(jedisPool);
        return jedisPool;
    }


    @Test
    public void testIfInterruptedFor5SecondsLock() throws InterruptedException, IOException {
        for (int i = 0; i < FUNCTIONAL_TEST_CYCLES; i ++) {
            line = 0;
            File tempFile = folder.newFile(getClass().getName() + "." + System.currentTimeMillis() + ".txt");
            log.info("Temp file is " + tempFile.getAbsolutePath());
            otherError.set(false);
            log.info("_\n");
            log.info("FUNCTIONAL_TEST_CYCLES " + i);
            Thread t1 = new Thread(new WriteTest(270, tempFile));
            t1.setName("T1_i"+i);
            Thread t2 = new Thread(new WriteTest(190, tempFile));
            t2.setName("T2_i"+i);
            Thread t3 = new Thread(new WriteTest(220, tempFile));
            t3.setName("T3_i"+i);

            List<Thread> threadList = Arrays.asList(t1,t2,t3);
            Collections.shuffle(threadList);
            threadList.forEach(Thread::start);
            for(Thread tt: threadList) tt.join();
            assertFalse(otherError.get());
            assertFalse(lockList.stream().anyMatch(il -> il != null && il.isLocked()));

        }
    }

    private class WriteTest implements Runnable {


        private IJedisLock jedisLock;
        private final long milis;
        private final File tempFile;


        WriteTest(long milis, File tempFile){
            this.milis = milis;
            this.tempFile = tempFile;
        }

        @Override
        public void run() {
            try  {
                JedisPool jedisPool = createJedisPoolAdapter();
                jedisLock = new JedisLock(jedisPool, lockName, milis, TimeUnit.MILLISECONDS);
                lockList.add(jedisLock);
                jedisLock.lock();
                checkLock(jedisLock);
                writeTest();
            } catch (java.nio.channels.ClosedByInterruptException cbie) {
                log.info("Closed channel by interrupt exception");
                Thread.interrupted();  // We clean the state
            } catch (Exception e){
                log.error("Error ", e);
                otherError.set(true);
            } finally {
                jedisLock.unlock();
                lockList.remove(jedisLock);
            }
        }

        private void writeTest() throws IOException {
            log.info("Writing with thread " + Thread.currentThread().getName());
            FileWriter fileWriter= null;
            PrintWriter printWriter = null;
            try {
                fileWriter = new FileWriter(tempFile, true);
                printWriter = new PrintWriter(fileWriter);
                while(jedisLock.isLocked()) {
                    line++;
                    printWriter.printf("#" + line + " " + Thread.currentThread().getName()+"\n");
                }
            } finally {
                if (printWriter!=null) printWriter.close();
                if (fileWriter!= null) fileWriter.close();
            }
        }
    }



}
