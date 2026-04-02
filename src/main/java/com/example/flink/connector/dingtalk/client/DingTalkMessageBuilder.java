package com.example.flink.connector.dingtalk.client;

import com.example.flink.connector.dingtalk.config.DingTalkSinkOptions;
import com.example.flink.connector.dingtalk.config.MessageType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;

/**
 * Builds DingTalk message JSON payloads based on message type and field values.
 */
public class DingTalkMessageBuilder {

    private final ObjectMapper mapper = new ObjectMapper();
    private final DingTalkSinkOptions options;

    public DingTalkMessageBuilder(DingTalkSinkOptions options) {
        this.options = options;
    }

    /**
     * Build a message payload from field values.
     *
     * @param fields map of field name -> value from the input record
     * @return JSON string of the message payload
     */
    public String buildMessage(Map<String, String> fields) {
        switch (options.getMessageType()) {
            case TEXT:
                return buildText(fields);
            case MARKDOWN:
                return buildMarkdown(fields);
            case ACTION_CARD:
                return buildActionCard(fields);
            case LINK:
                return buildLink(fields);
            default:
                throw new IllegalArgumentException("Unsupported message type: " + options.getMessageType());
        }
    }

    private String buildText(Map<String, String> fields) {
        ObjectNode root = mapper.createObjectNode();
        root.put("msgtype", "text");

        ObjectNode text = mapper.createObjectNode();
        String content = fields.getOrDefault("content", fields.getOrDefault("text", ""));
        text.put("content", content);
        root.set("text", text);

        // @ mentions
        if (options.isAtAll() || options.getAtMobiles() != null) {
            ObjectNode at = mapper.createObjectNode();
            at.put("isAtAll", options.isAtAll());
            if (options.getAtMobiles() != null) {
                ArrayNode mobiles = mapper.createArrayNode();
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
        ObjectNode root = mapper.createObjectNode();
        root.put("msgtype", "markdown");

        ObjectNode markdown = mapper.createObjectNode();
        String title = fields.getOrDefault("title", "Notification");
        String text = fields.getOrDefault("content", fields.getOrDefault("text", ""));
        markdown.put("title", title);
        markdown.put("text", text);
        root.set("markdown", markdown);

        // @ mentions
        if (options.isAtAll() || options.getAtMobiles() != null) {
            ObjectNode at = mapper.createObjectNode();
            at.put("isAtAll", options.isAtAll());
            if (options.getAtMobiles() != null) {
                ArrayNode mobiles = mapper.createArrayNode();
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
        ObjectNode root = mapper.createObjectNode();
        root.put("msgtype", "actionCard");

        ObjectNode actionCard = mapper.createObjectNode();
        actionCard.put("title", fields.getOrDefault("title", "Notification"));
        actionCard.put("text", fields.getOrDefault("content", fields.getOrDefault("text", "")));
        actionCard.put("btnOrientation", "0");

        // Optional: single button from fields
        if (fields.containsKey("actionUrl") || fields.containsKey("singleTitle")) {
            ArrayNode btns = mapper.createArrayNode();
            ObjectNode btn = mapper.createObjectNode();
            btn.put("title", fields.getOrDefault("singleTitle", "View Details"));
            btn.put("actionURL", fields.getOrDefault("actionUrl", ""));
            btns.add(btn);
            actionCard.set("btns", btns);
        }

        root.set("actionCard", actionCard);
        return root.toString();
    }

    private String buildLink(Map<String, String> fields) {
        ObjectNode root = mapper.createObjectNode();
        root.put("msgtype", "link");

        ObjectNode link = mapper.createObjectNode();
        link.put("title", fields.getOrDefault("title", "Notification"));
        link.put("text", fields.getOrDefault("content", fields.getOrDefault("text", "")));
        link.put("messageUrl", fields.getOrDefault("messageUrl", ""));
        link.put("picUrl", fields.getOrDefault("picUrl", ""));
        root.set("link", link);

        return root.toString();
    }
}
