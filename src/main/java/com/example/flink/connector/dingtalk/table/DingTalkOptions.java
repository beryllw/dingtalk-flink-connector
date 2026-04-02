package com.example.flink.connector.dingtalk.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * SQL options for the DingTalk Sink connector.
 */
public final class DingTalkOptions {

    private DingTalkOptions() {}

    public static final ConfigOption<String> WEBHOOK = ConfigOptions
            .key("webhook")
            .stringType()
            .noDefaultValue()
            .withDescription("DingTalk webhook URL");

    public static final ConfigOption<String> SECRET = ConfigOptions
            .key("secret")
            .stringType()
            .noDefaultValue()
            .withDescription("DingTalk webhook signing secret for HMAC-SHA256");

    public static final ConfigOption<String> APP_KEY = ConfigOptions
            .key("app-key")
            .stringType()
            .noDefaultValue()
            .withDescription("DingTalk enterprise application AppKey");

    public static final ConfigOption<String> APP_SECRET = ConfigOptions
            .key("app-secret")
            .stringType()
            .noDefaultValue()
            .withDescription("DingTalk enterprise application AppSecret");

    public static final ConfigOption<String> ROBOT_CODE = ConfigOptions
            .key("robot-code")
            .stringType()
            .noDefaultValue()
            .withDescription("DingTalk robot code (from app settings)");

    public static final ConfigOption<String> USER_IDS = ConfigOptions
            .key("user-ids")
            .stringType()
            .noDefaultValue()
            .withDescription("Comma-separated list of DingTalk user IDs to send messages to");

    public static final ConfigOption<String> SEND_MODE = ConfigOptions
            .key("send-mode")
            .stringType()
            .defaultValue("webhook")
            .withDescription("Send mode: 'webhook' or 'api'");

    public static final ConfigOption<String> MESSAGE_TYPE = ConfigOptions
            .key("message-type")
            .stringType()
            .defaultValue("text")
            .withDescription("Message type: 'text', 'markdown', 'actionCard', 'link'");

    public static final ConfigOption<String> AT_MOBILES = ConfigOptions
            .key("at-mobiles")
            .stringType()
            .noDefaultValue()
            .withDescription("Comma-separated list of mobile numbers to @");

    public static final ConfigOption<Boolean> AT_ALL = ConfigOptions
            .key("at-all")
            .booleanType()
            .defaultValue(false)
            .withDescription("Whether to @ everyone in the group");

    public static final ConfigOption<String> USER_ID_FIELD = ConfigOptions
            .key("user-id-field")
            .stringType()
            .noDefaultValue()
            .withDescription("Name of the field containing the DingTalk userid (for API mode)");

    public static final ConfigOption<Integer> MAX_RETRIES = ConfigOptions
            .key("max-retries")
            .intType()
            .defaultValue(3)
            .withDescription("Maximum number of retries on failure");

    public static final ConfigOption<Long> RETRY_DELAY_MS = ConfigOptions
            .key("retry-delay-ms")
            .longType()
            .defaultValue(1000L)
            .withDescription("Retry delay in milliseconds");

    // Async sink options (inherited from flink-connector-base)
    public static final ConfigOption<Integer> MAX_BATCH_SIZE = ConfigOptions
            .key("sink.batch.max-size")
            .intType()
            .defaultValue(500)
            .withDescription("Maximum number of elements per batch");

    public static final ConfigOption<Long> FLUSH_BUFFER_SIZE = ConfigOptions
            .key("sink.flush-buffer.size")
            .longType()
            .defaultValue(5 * 1024 * 1024L)
            .withDescription("Maximum buffer size in bytes before flushing");

    public static final ConfigOption<Integer> MAX_BUFFERED_REQUESTS = ConfigOptions
            .key("sink.requests.max-buffered")
            .intType()
            .defaultValue(10000)
            .withDescription("Maximum number of buffered requests");

    public static final ConfigOption<Long> FLUSH_BUFFER_TIMEOUT = ConfigOptions
            .key("sink.flush-buffer.timeout")
            .longType()
            .defaultValue(5000L)
            .withDescription("Maximum time in buffer before flushing (ms)");

    public static final ConfigOption<Integer> MAX_IN_FLIGHT_REQUESTS = ConfigOptions
            .key("sink.requests.max-inflight")
            .intType()
            .defaultValue(50)
            .withDescription("Maximum concurrent in-flight requests");
}
