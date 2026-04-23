package io.github.beryllw.dingtalk.connector.sink;

import io.github.beryllw.dingtalk.connector.client.DingTalkMessageBuilder;
import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.SendMode;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Flink Sink that sends records to DingTalk as messages.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li>WEBHOOK: Sends to a DingTalk group robot webhook URL</li>
 *   <li>API: Sends via DingTalk enterprise application API (supports single chat)</li>
 * </ul>
 *
 * @param <InputT> the type of the input element
 */
@PublicEvolving
public class DingTalkSink<InputT>
        extends AsyncSinkBase<InputT, String> {

    private static final long serialVersionUID = 1L;

    // Default values tuned for DingTalk's rate limiting (~20 messages/minute for webhook)
    private static final int DEFAULT_MAX_BATCH_SIZE = 20;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 100;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 5 * 1024 * 1024; // 5MB
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000; // 5s
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_BYTES = 1024 * 1024; // 1MB

    private final DingTalkSinkOptions options;

    protected DingTalkSink(
            ElementConverter<InputT, String> elementConverter,
            DingTalkSinkOptions options,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.options = options;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<String>> createWriter(
            WriterInitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<String>> restoreWriter(
            WriterInitContext context,
            Collection<BufferedRequestState<String>> recoveredState) throws IOException {
        org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration config =
                org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(getMaxBatchSize())
                        .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(getMaxInFlightRequests())
                        .setMaxBufferedRequests(getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
                        .build();

        return new DingTalkSinkWriter<>(
                getElementConverter(),
                options,
                context,
                config,
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<String>> getWriterStateSerializer() {
        return new StringStateSerializer();
    }

    public DingTalkSinkOptions getOptions() {
        return options;
    }

    /**
     * Creates a new builder for {@link DingTalkSink}.
     */
    public static <InputT> DingTalkSinkBuilder<InputT> builder() {
        return new DingTalkSinkBuilder<>();
    }

    /**
     * Builder for {@link DingTalkSink}.
     *
     * @param <InputT> the type of the input element
     */
    public static class DingTalkSinkBuilder<InputT>
            extends AsyncSinkBaseBuilder<InputT, String, DingTalkSinkBuilder<InputT>> {

        private DingTalkSinkOptions options;
        private ElementConverter<InputT, String> elementConverter;

        public DingTalkSinkBuilder<InputT> setOptions(DingTalkSinkOptions options) {
            this.options = options;
            return this;
        }

        /**
         * Set a custom element converter. If not set, a default converter will be used.
         */
        public DingTalkSinkBuilder<InputT> setElementConverter(
                ElementConverter<InputT, String> elementConverter) {
            this.elementConverter = elementConverter;
            return this;
        }

        public DingTalkSinkBuilder<InputT> setWebhook(String webhook) {
            ensureOptions();
            options.setWebhook(webhook);
            options.setSendMode(SendMode.WEBHOOK);
            return this;
        }

        public DingTalkSinkBuilder<InputT> setSecret(String secret) {
            ensureOptions();
            options.setSecret(secret);
            return this;
        }

        public DingTalkSinkBuilder<InputT> setAppKey(String appKey) {
            ensureOptions();
            options.setAppKey(appKey);
            options.setSendMode(SendMode.API);
            return this;
        }

        public DingTalkSinkBuilder<InputT> setAppSecret(String appSecret) {
            ensureOptions();
            options.setAppSecret(appSecret);
            return this;
        }

        public DingTalkSinkBuilder<InputT> setMessageType(
                io.github.beryllw.dingtalk.connector.config.MessageType messageType) {
            ensureOptions();
            options.setMessageType(messageType);
            return this;
        }

        private void ensureOptions() {
            if (options == null) {
                options = new DingTalkSinkOptions();
            }
        }

        @Override
        public DingTalkSink<InputT> build() {
            if (options == null) {
                throw new IllegalArgumentException(
                        "Options must be set via setWebhook() or setAppKey()/setAppSecret()");
            }

            ElementConverter<InputT, String> converter =
                    elementConverter != null ? elementConverter : createElementConverter(options);

            return new DingTalkSink<>(
                    converter,
                    options,
                    getMaxBatchSize() != null ? getMaxBatchSize() : DEFAULT_MAX_BATCH_SIZE,
                    getMaxInFlightRequests() != null ? getMaxInFlightRequests() : DEFAULT_MAX_IN_FLIGHT_REQUESTS,
                    getMaxBufferedRequests() != null ? getMaxBufferedRequests() : DEFAULT_MAX_BUFFERED_REQUESTS,
                    getMaxBatchSizeInBytes() != null ? getMaxBatchSizeInBytes() : DEFAULT_MAX_BATCH_SIZE_IN_BYTES,
                    getMaxTimeInBufferMS() != null ? getMaxTimeInBufferMS() : DEFAULT_MAX_TIME_IN_BUFFER_MS,
                    getMaxRecordSizeInBytes() != null ? getMaxRecordSizeInBytes() : DEFAULT_MAX_RECORD_SIZE_IN_BYTES);
        }

        @SuppressWarnings("unchecked")
        private static <InputT> ElementConverter<InputT, String> createElementConverter(
                DingTalkSinkOptions options) {
            DingTalkMessageBuilder messageBuilder = new DingTalkMessageBuilder(options);

            return (element, context) -> {
                if (element instanceof Map) {
                    return messageBuilder.buildMessage((Map<String, String>) element);
                }
                return messageBuilder.buildMessage(
                        Collections.singletonMap("content", element.toString()));
            };
        }
    }

    /**
     * SimpleVersionedSerializer for String-based request entries.
     */
    private static class StringStateSerializer extends AsyncSinkWriterStateSerializer<String> {
        @Override
        protected void serializeRequestToStream(String request, DataOutputStream out)
                throws IOException {
            byte[] bytes = request.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        @Override
        protected String deserializeRequestFromStream(long requestSize, DataInputStream in)
                throws IOException {
            byte[] bytes = new byte[(int) requestSize];
            in.readFully(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }
}
