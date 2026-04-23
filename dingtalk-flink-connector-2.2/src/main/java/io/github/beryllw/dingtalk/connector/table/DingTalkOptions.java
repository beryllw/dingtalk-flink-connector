package io.github.beryllw.dingtalk.connector.table;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import io.github.beryllw.dingtalk.connector.config.SendMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.Map;

/**
 * SQL options for the DingTalk Sink connector.
 */
public final class DingTalkOptions {

    private DingTalkOptions() {}

    /**
     * Parse options from a map of string key-value pairs (e.g., from SQL table options).
     */
    public static DingTalkSinkOptions parse(Map<String, String> options) {
        Configuration config = Configuration.fromMap(options);
        DingTalkSinkOptions sinkOptions = new DingTalkSinkOptions();

        sinkOptions.setWebhook(config.get(WEBHOOK));
        sinkOptions.setSecret(config.get(SECRET));
        sinkOptions.setAppKey(config.get(APP_KEY));
        sinkOptions.setAppSecret(config.get(APP_SECRET));
        sinkOptions.setRobotCode(config.get(ROBOT_CODE));

        String userIds = config.get(USER_IDS);
        if (userIds != null) {
            sinkOptions.setUserIds(Arrays.asList(userIds.split(",")));
        }

        String sendMode = config.get(SEND_MODE);
        if (sendMode != null) {
            sinkOptions.setSendMode(SendMode.valueOf(sendMode.toUpperCase()));
        }

        String messageType = config.get(MESSAGE_TYPE);
        if (messageType != null) {
            sinkOptions.setMessageType(MessageType.fromValue(messageType));
        }

        String atMobiles = config.get(AT_MOBILES);
        if (atMobiles != null) {
            sinkOptions.setAtMobiles(Arrays.asList(atMobiles.split(",")));
        }

        sinkOptions.setAtAll(config.get(AT_ALL));
        sinkOptions.setUserIdField(config.get(USER_ID_FIELD));
        sinkOptions.setMaxRetries(config.get(MAX_RETRIES));
        sinkOptions.setRetryDelayMs(config.get(RETRY_DELAY_MS));

        return sinkOptions;
    }

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

    // ---- Async sink parameters (DingTalk-tuned defaults) ----

    public static final ConfigOption<Integer> SINK_BATCH_MAX_SIZE = ConfigOptions
            .key("sink.batch.max-size")
            .intType()
            .defaultValue(20)
            .withDescription("Maximum number of records per batch. "
                    + "Default 20, matching DingTalk's ~20 messages/minute rate limit.");

    public static final ConfigOption<Integer> SINK_MAX_IN_FLIGHT_REQUESTS = ConfigOptions
            .key("sink.max-in-flight-requests")
            .intType()
            .defaultValue(1)
            .withDescription("Maximum number of in-flight async requests. "
                    + "Default 1, to serialize requests and avoid DingTalk rate limiting.");

    public static final ConfigOption<Integer> SINK_MAX_BUFFERED_REQUESTS = ConfigOptions
            .key("sink.max-buffered-requests")
            .intType()
            .defaultValue(100)
            .withDescription("Maximum number of buffered requests before back-pressure. Default 100.");

    public static final ConfigOption<Long> SINK_BUFFER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.buffer.flush-interval")
            .longType()
            .defaultValue(5000L)
            .withDescription("Maximum time in milliseconds a record stays in the buffer before flushing. Default 5000.");

    public static final ConfigOption<Long> SINK_BUFFER_MAX_SIZE_IN_BYTES = ConfigOptions
            .key("sink.buffer.max-size-in-bytes")
            .longType()
            .defaultValue(5 * 1024 * 1024L)
            .withDescription("Maximum size of the buffer in bytes. Default 5 MB.");
}
