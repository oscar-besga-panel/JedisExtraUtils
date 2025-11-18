package org.oba.jedis.extra.utils.lock;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class UniqueTokenValueGenerator {

    public static String generateUniqueTokenValue(String name) {
        return name + "_" + System.nanoTime() + "_" +
                UUID.randomUUID().toString().replace("-", "_") +
                "_" + ThreadLocalRandom.current().nextInt(1_000_000);
    }

}