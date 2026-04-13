package io.github.beryllw.dingtalk.connector.table;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import io.github.beryllw.dingtalk.connector.config.SendMode;
import io.github.beryllw.dingtalk.connector.sink.DingTalkSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table sink that converts {@link RowData} to DingTalk messages.
 */
public class DingTalkTableSink extends AsyncDynamicTableSink<String>
        implements SupportsWritingMetadata {

    private final DingTalkSinkOptions options;
    private final DataType physicalDataType;
    private final String[] fieldNames;
    private final SerializationSchema<RowData> defaultSerializationSchema;
    private final boolean useExternalFormat;

    // Metadata fields
    private List<String> metadataKeys = Collections.emptyList();
    private DataType consumedDataType;

    public DingTalkTableSink(
            DingTalkSinkOptions options,
            DataType physicalDataType,
            String[] fieldNames,
            @Nullable SerializationSchema<RowData> defaultSerializationSchema,
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            boolean useExternalFormat) {
        super(maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBufferSizeInBytes, maxTimeInBufferMS);
        this.options = options;
        this.physicalDataType = physicalDataType;
        this.fieldNames = fieldNames;
        this.defaultSerializationSchema = defaultSerializationSchema;
        this.useExternalFormat = useExternalFormat;
    }

    private DingTalkTableSink(
            DingTalkSinkOptions options,
            DataType physicalDataType,
            String[] fieldNames,
            List<String> metadataKeys,
            DataType consumedDataType,
            @Nullable SerializationSchema<RowData> defaultSerializationSchema,
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS,
            boolean useExternalFormat) {
        super(maxBatchSize, maxInFlightRequests, maxBufferedRequests, maxBufferSizeInBytes, maxTimeInBufferMS);
        this.options = options;
        this.physicalDataType = physicalDataType;
        this.fieldNames = fieldNames;
        this.defaultSerializationSchema = defaultSerializationSchema;
        this.useExternalFormat = useExternalFormat;
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializer = getSerializer(context);
        DingTalkSink<RowData> sink = DingTalkSink.<RowData>builder()
                .setOptions(options)
                .setElementConverter(createRowDataElementConverter(serializer))
                .build();

        return SinkV2Provider.of(sink);
    }

    private SerializationSchema<RowData> getSerializer(Context context) {
        if (useExternalFormat) {
            return new FormatDelegatingSerializer(context, physicalDataType);
        }
        return defaultSerializationSchema;
    }

    private org.apache.flink.connector.base.sink.writer.ElementConverter<RowData, String> createRowDataElementConverter(
            SerializationSchema<RowData> serializer) {
        return new RowDataElementConverter(options, serializer, fieldNames);
    }

    @Override
    public DynamicTableSink copy() {
        return new DingTalkTableSink(
                options,
                physicalDataType,
                fieldNames,
                metadataKeys,
                consumedDataType,
                defaultSerializationSchema,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS,
                useExternalFormat);
    }

    @Override
    public String asSummaryString() {
        return "DingTalk Sink";
    }

    // --- Metadata Support ---

    private static final Map<String, DataType> SUPPORTED_METADATA = new LinkedHashMap<>();
    static {
        SUPPORTED_METADATA.put("timestamp", org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ(3));
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return SUPPORTED_METADATA;
    }

    @Override
    public void applyWritableMetadata(List<String> metadata, DataType consumedDataType) {
        this.metadataKeys = metadata;
        this.consumedDataType = consumedDataType;
    }

    /**
     * ElementConverter that converts RowData to a DingTalk message JSON string.
     */
    private static class RowDataElementConverter
            implements org.apache.flink.connector.base.sink.writer.ElementConverter<RowData, String> {

        private static final long serialVersionUID = 1L;

        private final DingTalkSinkOptions options;
        private final SerializationSchema<RowData> serializationSchema;
        private final String[] fieldNames;

        RowDataElementConverter(
                DingTalkSinkOptions options,
                SerializationSchema<RowData> serializationSchema,
                String[] fieldNames) {
            this.options = options;
            this.serializationSchema = serializationSchema;
            this.fieldNames = fieldNames;
        }

        @Override
        public String apply(RowData element, org.apache.flink.api.connector.sink2.SinkWriter.Context context) {
            // Build field map directly from RowData instead of parsing serializer output
            Map<String, String> fields = new HashMap<>();
            int arity = element.getArity();
            for (int i = 0; i < arity; i++) {
                String name = fieldNames != null && i < fieldNames.length ? fieldNames[i] : "field" + i;
                if (element.isNullAt(i)) {
                    fields.put(name, "");
                } else {
                    fields.put(name, element.getString(i).toString());
                }
            }

            io.github.beryllw.dingtalk.connector.client.DingTalkMessageBuilder messageBuilder =
                    new io.github.beryllw.dingtalk.connector.client.DingTalkMessageBuilder(options);
            return messageBuilder.buildMessage(fields);
        }
    }

    /**
     * Serializer that delegates to an external format (e.g., JSON format).
     */
    private static class FormatDelegatingSerializer implements SerializationSchema<RowData> {
        private static final long serialVersionUID = 1L;
        private final Context context;
        private final DataType physicalDataType;

        FormatDelegatingSerializer(Context context, DataType physicalDataType) {
            this.context = context;
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
