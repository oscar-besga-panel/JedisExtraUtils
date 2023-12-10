package org.oba.jedis.extra.utils.lock;

import java.util.concurrent.ThreadLocalRandom;

public class UniqueTokenValueGenerator {

    private static long lastCurrentTimeMilis = 0L;

    public synchronized static String generateUniqueTokenValue(String name){
        long currentTimeMillis = System.currentTimeMillis();
        try {
            while(currentTimeMillis == lastCurrentTimeMilis){
                Thread.sleep(1);
                currentTimeMillis = System.currentTimeMillis();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        lastCurrentTimeMilis = currentTimeMillis;
        return name + "_" + System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt(1_000_000);
    }

}
