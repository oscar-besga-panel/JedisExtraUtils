package org.oba.jedis.extra.utils.utils;

import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.UnifiedJedis;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Class to aid with transactions and help coders to open/close a transaction
 */
public class TransactionUtil {


    /**
     * Creates a transaction with multi for the redis client and applies the action
     * It open/closes the transaction automatically
     * @param redisClient Connection
     * @param action lambda to execute
     */
    public static void withinMultiDo(UnifiedJedis redisClient, Consumer<AbstractTransaction> action) {
        try(AbstractTransaction transaction = redisClient.multi()) {
            action.accept(transaction);
        }
    }

        /**
         * Creates a transaction with multi for the redis client and applies the action,
         * returning the result of the function
         * It open/closes the transaction automatically
         * @param redisClient Connection
         * @param action lambda to execute
         * @return response
         */
    public static <R> R withinMultiGet(UnifiedJedis redisClient, Function<AbstractTransaction, R> action) {
        try(AbstractTransaction transaction = redisClient.multi()) {
            return action.apply(transaction);
        }
    }


}
