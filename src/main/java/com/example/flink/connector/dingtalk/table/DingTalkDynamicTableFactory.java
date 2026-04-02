package com.example.flink.connector.dingtalk.table;

import com.example.flink.connector.dingtalk.config.DingTalkSinkOptions;
import com.example.flink.connector.dingtalk.config.MessageType;
import com.example.flink.connector.dingtalk.config.SendMode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.example.flink.connector.dingtalk.table.DingTalkOptions.APP_KEY;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.APP_SECRET;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.AT_ALL;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.AT_MOBILES;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.FLUSH_BUFFER_SIZE;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.FLUSH_BUFFER_TIMEOUT;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.MAX_BATCH_SIZE;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.MAX_BUFFERED_REQUESTS;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.MAX_IN_FLIGHT_REQUESTS;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.MAX_RETRIES;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.MESSAGE_TYPE;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.RETRY_DELAY_MS;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.ROBOT_CODE;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.SECRET;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.SEND_MODE;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.USER_ID_FIELD;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.USER_IDS;
import static com.example.flink.connector.dingtalk.table.DingTalkOptions.WEBHOOK;

/**
 * Factory for creating {@link DingTalkTableSink} instances via SQL.
 */
public class DingTalkDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "dingtalk";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> requiredOptions() {
        // Either webhook OR (app-key + app-secret) is required
        // We declare none as required and validate in createDynamicTableSink
        return Collections.emptySet();
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> optionalOptions() {
        Set<org.apache.flink.configuration.ConfigOption<?>> options = new HashSet<>();
        options.add(WEBHOOK);
        options.add(SECRET);
        options.add(APP_KEY);
        options.add(APP_SECRET);
        options.add(ROBOT_CODE);
        options.add(USER_IDS);
        options.add(SEND_MODE);
        options.add(MESSAGE_TYPE);
        options.add(AT_MOBILES);
        options.add(AT_ALL);
        options.add(USER_ID_FIELD);
        options.add(MAX_RETRIES);
        options.add(RETRY_DELAY_MS);
        options.add(MAX_BATCH_SIZE);
        options.add(FLUSH_BUFFER_SIZE);
        options.add(MAX_BUFFERED_REQUESTS);
        options.add(FLUSH_BUFFER_TIMEOUT);
        options.add(MAX_IN_FLIGHT_REQUESTS);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();

        // Validate that either webhook or app-key+app-secret is provided
        String webhook = config.get(WEBHOOK);
        String appKey = config.get(APP_KEY);
        String appSecret = config.get(APP_SECRET);

        if (webhook == null && (appKey == null || appSecret == null)) {
            throw new IllegalArgumentException(
                    "Either 'webhook' or both 'app-key' and 'app-secret' must be specified");
        }

        // Build options
        DingTalkSinkOptions options = buildOptions(config);

        DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        // Check if a format is configured - we'll create the encoder in the sink
        // where DynamicTableSink.Context is available
        boolean hasFormat = helper.discoverOptionalEncodingFormat(
                SerializationFormatFactory.class, FactoryUtil.FORMAT).isPresent();

        return new DingTalkTableSink(
                options,
                physicalDataType,
                hasFormat ? null : createDefaultSerializer(physicalDataType),
                config.get(MAX_BATCH_SIZE),
                config.get(MAX_IN_FLIGHT_REQUESTS),
                config.get(MAX_BUFFERED_REQUESTS),
                config.get(FLUSH_BUFFER_SIZE),
                config.get(FLUSH_BUFFER_TIMEOUT),
                hasFormat);
    }

    private DingTalkSinkOptions buildOptions(ReadableConfig config) {
        DingTalkSinkOptions options = new DingTalkSinkOptions();

        String sendMode = config.get(SEND_MODE);
        options.setSendMode(SendMode.valueOf(sendMode.toUpperCase()));

        options.setWebhook(config.get(WEBHOOK));
        options.setSecret(config.get(SECRET));
        options.setAppKey(config.get(APP_KEY));
        options.setAppSecret(config.get(APP_SECRET));
        options.setRobotCode(config.get(ROBOT_CODE));
        String userIds = config.get(USER_IDS);
        if (userIds != null && !userIds.isEmpty()) {
            options.setUserIds(Arrays.asList(userIds.split(",")));
        }

        String messageType = config.get(MESSAGE_TYPE);
        options.setMessageType(MessageType.valueOf(messageType.toUpperCase().replace("-", "_")));

        String atMobiles = config.get(AT_MOBILES);
        if (atMobiles != null && !atMobiles.isEmpty()) {
            options.setAtMobiles(Arrays.asList(atMobiles.split(",")));
        }

        options.setAtAll(config.get(AT_ALL));
        options.setUserIdField(config.get(USER_ID_FIELD));
        options.setMaxRetries(config.get(MAX_RETRIES));
        options.setRetryDelayMs(config.get(RETRY_DELAY_MS));

        return options;
    }

    private SerializationSchema<RowData> createDefaultSerializer(DataType physicalDataType) {
        return new SimpleJsonSerializationSchema(physicalDataType);
    }

    /**
     * Simple JSON serialization schema that converts RowData to JSON.
     */
    private static class SimpleJsonSerializationSchema implements SerializationSchema<RowData> {
        private static final long serialVersionUID = 1L;
        private final DataType physicalDataType;

        SimpleJsonSerializationSchema(DataType physicalDataType) {
            this.physicalDataType = physicalDataType;
        }

        @Override
        public byte[] serialize(RowData row) {
            try {
                com.fasterxml.jackson.databind.node.ObjectNode node =
                        com.fasterxml.jackson.databind.json.JsonMapper.builder().build().createObjectNode();

                int arity = row.getArity();
                for (int i = 0; i < arity; i++) {
                    String fieldName = "field" + i;
                    if (row.isNullAt(i)) {
                        node.putNull(fieldName);
                    } else {
                        node.put(fieldName, row.getString(i).toString());
                    }
                }
                return node.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize RowData to JSON", e);
            }
        }
    }
}
