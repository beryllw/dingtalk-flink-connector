package io.github.beryllw.dingtalk.connector.config;

import java.util.HashMap;
import java.util.Map;

/**
 * DingTalk message type.
 *
 * <p>SQL option values: {@code text}, {@code markdown}, {@code actionCard}, {@code link}.
 */
public enum MessageType {
    TEXT("text"),
    MARKDOWN("markdown"),
    ACTION_CARD("actionCard"),
    LINK("link");

    private final String value;

    private static final Map<String, MessageType> VALUE_MAP = new HashMap<>();

    static {
        for (MessageType type : values()) {
            VALUE_MAP.put(type.value.toLowerCase(), type);
        }
    }

    MessageType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Parse a message type from the SQL option value.
     *
     * @param value one of {@code text}, {@code markdown}, {@code actionCard}, {@code link}
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static MessageType fromValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "message-type must not be null. "
                            + "Supported values: text, markdown, actionCard, link");
        }
        MessageType type = VALUE_MAP.get(value.toLowerCase());
        if (type == null) {
            throw new IllegalArgumentException(
                    "Unsupported message-type: '" + value + "'. "
                            + "Supported values: text, markdown, actionCard, link");
        }
        return type;
    }
}
