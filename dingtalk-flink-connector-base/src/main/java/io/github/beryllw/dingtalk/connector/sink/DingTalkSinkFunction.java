package io.github.beryllw.dingtalk.connector.sink;

import io.github.beryllw.dingtalk.connector.client.DingTalkClient;
import io.github.beryllw.dingtalk.connector.client.DingTalkMessageBuilder;
import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.SendMode;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Simple SinkWriter implementation for DingTalk.
 *
 * @param <T> the type of the input element
 */
public class DingTalkSinkFunction<T> implements SinkWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DingTalkSinkFunction.class);

    private final DingTalkSinkOptions options;
    private final DingTalkClient client;
    private final DingTalkMessageBuilder messageBuilder;

    public DingTalkSinkFunction(DingTalkSinkOptions options) {
        this.options = options;
        if (options.getSendMode() == SendMode.API) {
            client = new io.github.beryllw.dingtalk.connector.client.DingTalkApiClient(options);
        } else {
            client = new io.github.beryllw.dingtalk.connector.client.DingTalkWebhookClient(options);
        }
        messageBuilder = new DingTalkMessageBuilder(options);
    }

    @Override
    public void write(T element, Context context) throws IOException, InterruptedException {
        Map<String, String> fields;
        if (element instanceof Map) {
            fields = (Map<String, String>) element;
        } else {
            fields = Collections.singletonMap("content", element.toString());
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
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // No-op: each write is immediately sent
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
