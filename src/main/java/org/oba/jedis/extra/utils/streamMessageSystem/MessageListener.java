package org.oba.jedis.extra.utils.streamMessageSystem;

/**
 * Interface to listen to a message from a stream
 */
public interface MessageListener {

    /**
     * Recieve a message
     * @param message Data recieved from stream
     */
    void onMessage(String message);

}
