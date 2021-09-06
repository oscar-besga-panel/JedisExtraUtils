package org.obapanel.jedis.common.test;


import redis.clients.jedis.Builder;
import redis.clients.jedis.Response;

import java.util.function.Supplier;

public final class TransactionOrder<T> extends Builder<T> {

    public static <Q> TransactionOrder<Q> quickTransactionOrder(final Q data) {
        return new TransactionOrder<>(() -> data);
    }


    public static <Q> Response<Q> quickReponse(Q data) {
        return quickTransactionOrder(data).getResponse();
    }

    public static <Q> Response<Q> quickReponseExecuted(Q data) {
        TransactionOrder<Q> transactionOrder = quickTransactionOrder(data);
        transactionOrder.execute();
        return transactionOrder.getResponse();
    }


    private final Response<T> response;
    private final Supplier<T> supplier;
    private T result;


    public TransactionOrder(Supplier<T> callable) {
        this.response = new Response<>(this);
        this.supplier = callable;
    }

    public void execute(){
        result = supplier.get();
        response.set(result);
    }

    @Override
    public T build(Object data) {
        if (result == null) {
            execute();
        }
        return result;
    }

    public Response<T> getResponse(){
        return response;
    }
}
