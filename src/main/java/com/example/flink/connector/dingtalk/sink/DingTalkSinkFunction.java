package com.example.flink.connector.dingtalk.sink;

import com.example.flink.connector.dingtalk.client.DingTalkClient;
import com.example.flink.connector.dingtalk.client.DingTalkMessageBuilder;
import com.example.flink.connector.dingtalk.config.DingTalkSinkOptions;
import com.example.flink.connector.dingtalk.config.SendMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Legacy RichSinkFunction wrapper for DingTalk.
 * Use this for DataStream API when you need a simple sink function.
 *
 * @param <T> the type of the input element
 */
public class DingTalkSinkFunction<T> extends RichSinkFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DingTalkSinkFunction.class);

    private final DingTalkSinkOptions options;

    private transient DingTalkClient client;
    private transient DingTalkMessageBuilder messageBuilder;

    public DingTalkSinkFunction(DingTalkSinkOptions options) {
        this.options = options;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (options.getSendMode() == SendMode.API) {
            client = new com.example.flink.connector.dingtalk.client.DingTalkApiClient(options);
        } else {
            client = new com.example.flink.connector.dingtalk.client.DingTalkWebhookClient(options);
        }
        messageBuilder = new DingTalkMessageBuilder(options);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        Map<String, String> fields;
        if (value instanceof Map) {
            fields = (Map<String, String>) value;
        } else {
            fields = Collections.singletonMap("content", value.toString());
        }

        String payload = messageBuilder.buildMessage(fields);

        int retries = 0;
        Exception lastException = null;
        while (retries <= options.getMaxRetries()) {
            try {
                String response = client.send(payload);
                LOG.debug("DingTalk response: {}", response);
                return;
            } catch (Exception e) {
                lastException = e;
                retries++;
                if (retries <= options.getMaxRetries()) {
                    long delay = options.getRetryDelayMs() * (1L << (retries - 1));
                    Thread.sleep(delay);
                }
            }
        }
        throw new RuntimeException("Failed to send DingTalk message after " + options.getMaxRetries() + " retries", lastException);
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
        super.close();
    }
}
