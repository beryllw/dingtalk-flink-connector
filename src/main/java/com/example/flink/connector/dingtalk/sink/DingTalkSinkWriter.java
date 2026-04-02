package com.example.flink.connector.dingtalk.sink;

import com.example.flink.connector.dingtalk.client.DingTalkApiClient;
import com.example.flink.connector.dingtalk.client.DingTalkClient;
import com.example.flink.connector.dingtalk.client.DingTalkWebhookClient;
import com.example.flink.connector.dingtalk.config.DingTalkSinkOptions;
import com.example.flink.connector.dingtalk.config.SendMode;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Writer for {@link DingTalkSink} that sends JSON payloads to DingTalk.
 *
 * @param <InputT> the type of the input element
 */
public class DingTalkSinkWriter<InputT>
        extends AsyncSinkWriter<InputT, String> {

    private static final Logger LOG = LoggerFactory.getLogger(DingTalkSinkWriter.class);

    private final DingTalkClient client;
    private final int maxRetries;
    private final long retryDelayMs;

    public DingTalkSinkWriter(
            ElementConverter<InputT, String> elementConverter,
            DingTalkSinkOptions options,
            Sink.InitContext context,
            AsyncSinkWriterConfiguration writerConfig,
            Collection<BufferedRequestState<String>> states)
            throws IOException {
        super(elementConverter, context, writerConfig, states);

        this.client = createClient(options);
        this.maxRetries = options.getMaxRetries();
        this.retryDelayMs = options.getRetryDelayMs();
    }

    @Override
    protected void submitRequestEntries(
            List<String> requestEntries,
            java.util.function.Consumer<List<String>> requestToRetry) {

        List<String> failed = new ArrayList<>();

        for (String entry : requestEntries) {
            boolean success = false;
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    String response = client.send(entry);
                    LOG.debug("DingTalk send response: {}", response);

                    // Check for error in response
                    if (response != null && response.contains("\"errcode\":0")) {
                        success = true;
                        break;
                    } else if (response != null && response.contains("\"errcode\"")) {
                        // DingTalk returned an error, retry for certain codes
                        if (shouldRetry(response) && attempt < maxRetries) {
                            LOG.warn("DingTalk returned error (attempt {}): {}", attempt + 1, response);
                            sleepBeforeRetry(attempt);
                            continue;
                        }
                        LOG.error("DingTalk send failed with error: {}", response);
                        failed.add(entry);
                        break;
                    } else {
                        // Webhook mode returns success without errcode:0
                        success = true;
                        break;
                    }
                } catch (IOException e) {
                    LOG.warn("DingTalk send failed (attempt {}): {}", attempt + 1, e.getMessage());
                    if (attempt < maxRetries) {
                        sleepBeforeRetry(attempt);
                    } else {
                        LOG.error("DingTalk send failed after {} retries", maxRetries, e);
                        failed.add(entry);
                    }
                }
            }
            if (!success && !failed.contains(entry)) {
                failed.add(entry);
            }
        }

        if (!failed.isEmpty()) {
            LOG.warn("Failed to send {} messages to DingTalk", failed.size());
            requestToRetry.accept(failed);
        }
    }

    @Override
    protected long getSizeInBytes(String requestEntry) {
        return requestEntry.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            LOG.warn("Failed to close DingTalk client", e);
        }
        super.close();
    }

    private static DingTalkClient createClient(DingTalkSinkOptions options) {
        if (options.getSendMode() == SendMode.API) {
            return new DingTalkApiClient(options);
        }
        return new DingTalkWebhookClient(options);
    }

    private boolean shouldRetry(String response) {
        // Retry on rate limiting and transient errors
        return response.contains("42001")  // token expired
                || response.contains("40014")  // invalid token
                || response.contains("40001")  // access_token not exist
                || response.contains("45009")  // rate limit
                || response.contains("42014");  // invalid param
    }

    private void sleepBeforeRetry(int attempt) {
        try {
            long delay = retryDelayMs * (1L << attempt);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
