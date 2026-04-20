package io.github.beryllw.dingtalk.connector.client;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Builds DingTalk message JSON payloads based on message type and field values.
 * Serializable for use in Flink distributed execution.
 */
public class DingTalkMessageBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DingTalkSinkOptions options;
    private transient com.fasterxml.jackson.databind.ObjectMapper mapper;

    public DingTalkMessageBuilder(DingTalkSinkOptions options) {
        this.options = options;
    }

    private com.fasterxml.jackson.databind.ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        }
        return mapper;
    }

    /**
     * Internal key used to embed dynamic userIds within the payload JSON.
     * The DingTalkApiClient will extract this before sending.
     */
    public static final String DYNAMIC_USER_IDS_KEY = "__userIds__";

    /**
     * Build a message payload from field values.
     */
    public String buildMessage(Map<String, String> fields) {
        return buildMessage(fields, null);
    }

    /**
     * Build a message payload from field values with an optional dynamic userId.
     * When dynamicUserId is provided, it will be embedded into the payload for
     * the API client to extract and use as the target recipient.
     */
    public String buildMessage(Map<String, String> fields, String dynamicUserId) {
        String message;
        switch (options.getMessageType()) {
            case TEXT:
                message = buildText(fields);
                break;
            case MARKDOWN:
                message = buildMarkdown(fields);
                break;
            case ACTION_CARD:
                message = buildActionCard(fields);
                break;
            case LINK:
                message = buildLink(fields);
                break;
            default:
                throw new IllegalArgumentException("Unsupported message type: " + options.getMessageType());
        }

        // Embed dynamic userId into the payload if provided
        if (dynamicUserId != null && !dynamicUserId.isEmpty()) {
            try {
                com.fasterxml.jackson.databind.JsonNode root = mapper().readTree(message);
                ((ObjectNode) root).put(DYNAMIC_USER_IDS_KEY, dynamicUserId);
                return mapper().writeValueAsString(root);
            } catch (Exception e) {
                // Fall back to original message if embedding fails
                return message;
            }
        }
        return message;
    }

    private String buildText(Map<String, String> fields) {
        ObjectNode root = mapper().createObjectNode();
        root.put("msgtype", "text");

        ObjectNode text = mapper().createObjectNode();
        String content = fields.getOrDefault("content", fields.getOrDefault("text", ""));
        text.put("content", content);
        root.set("text", text);

        if (options.isAtAll() || options.getAtMobiles() != null) {
            ObjectNode at = mapper().createObjectNode();
            at.put("isAtAll", options.isAtAll());
            if (options.getAtMobiles() != null) {
                ArrayNode mobiles = mapper().createArrayNode();
                for (String mobile : options.getAtMobiles()) {
                    mobiles.add(mobile);
                }
                at.set("atMobiles", mobiles);
            }
            root.set("at", at);
        }

        return root.toString();
    }

    private String buildMarkdown(Map<String, String> fields) {
        ObjectNode root = mapper().createObjectNode();
        root.put("msgtype", "markdown");

        ObjectNode markdown = mapper().createObjectNode();
        String title = fields.getOrDefault("title", "Notification");
        String text = fields.getOrDefault("content", fields.getOrDefault("text", ""));
        markdown.put("title", title);
        markdown.put("text", text);
        root.set("markdown", markdown);

        if (options.isAtAll() || options.getAtMobiles() != null) {
            ObjectNode at = mapper().createObjectNode();
            at.put("isAtAll", options.isAtAll());
            if (options.getAtMobiles() != null) {
                ArrayNode mobiles = mapper().createArrayNode();
                for (String mobile : options.getAtMobiles()) {
                    mobiles.add(mobile);
                }
                at.set("atMobiles", mobiles);
            }
            root.set("at", at);
        }

        return root.toString();
    }

    private String buildActionCard(Map<String, String> fields) {
        ObjectNode root = mapper().createObjectNode();
        root.put("msgtype", "actionCard");

        ObjectNode actionCard = mapper().createObjectNode();
        actionCard.put("title", fields.getOrDefault("title", "Notification"));
        actionCard.put("text", fields.getOrDefault("content", fields.getOrDefault("text", "")));
        actionCard.put("btnOrientation", "0");

        if (fields.containsKey("actionUrl") || fields.containsKey("singleTitle")) {
            ArrayNode btns = mapper().createArrayNode();
            ObjectNode btn = mapper().createObjectNode();
            btn.put("title", fields.getOrDefault("singleTitle", "View Details"));
            btn.put("actionURL", fields.getOrDefault("actionUrl", ""));
            btns.add(btn);
            actionCard.set("btns", btns);
        }

        root.set("actionCard", actionCard);
        return root.toString();
    }

    private String buildLink(Map<String, String> fields) {
        ObjectNode root = mapper().createObjectNode();
        root.put("msgtype", "link");

        ObjectNode link = mapper().createObjectNode();
        link.put("title", fields.getOrDefault("title", "Notification"));
        link.put("text", fields.getOrDefault("content", fields.getOrDefault("text", "")));
        link.put("messageUrl", fields.getOrDefault("messageUrl", ""));
        link.put("picUrl", fields.getOrDefault("picUrl", ""));
        root.set("link", link);

        return root.toString();
    }
}
