package io.github.beryllw.dingtalk.connector.client;

import java.io.IOException;

/**
 * Interface for sending messages to DingTalk.
 */
public interface DingTalkClient {

    /**
     * Send a message payload to DingTalk.
     *
     * @param payload JSON message body
     * @return response body from DingTalk API
     * @throws IOException if the request fails
     */
    String send(String payload) throws IOException;

    /**
     * Close the client and release resources.
     */
    void close() throws IOException;
}
