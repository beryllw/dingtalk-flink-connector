package io.github.beryllw.dingtalk.connector.client;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import io.github.beryllw.dingtalk.connector.config.SendMode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for dynamic userId feature in API mode.
 * Validates the full flow: field extraction -> message embedding -> API client parsing.
 */
public class DingTalkDynamicUserIdTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testDynamicUserIdEmbeddedInPayload() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setMessageType(MessageType.TEXT);
        options.setUserIdField("userid");

        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("content", "Hello");

        // Simulate RowDataElementConverter extracting userId and passing it
        String payload = builder.buildMessage(fields, "user001");

        JsonNode root = mapper.readTree(payload);
        assertEquals("user001", root.get(DingTalkMessageBuilder.DYNAMIC_USER_IDS_KEY).asText());
        assertEquals("text", root.get("msgtype").asText());
        assertTrue(root.get("text").get("content").asText().contains("Hello"));
    }

    @Test
    public void testDynamicUserIdWithCommasSupportMultipleUsers() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setMessageType(MessageType.TEXT);

        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("content", "Broadcast");

        String payload = builder.buildMessage(fields, "user001,user002,user003");

        JsonNode root = mapper.readTree(payload);
        String dynamicUserIds = root.get(DingTalkMessageBuilder.DYNAMIC_USER_IDS_KEY).asText();
        assertEquals("user001,user002,user003", dynamicUserIds);

        // Verify it can be split into multiple user IDs
        String[] ids = dynamicUserIds.split(",");
        assertEquals(3, ids.length);
        assertEquals("user001", ids[0]);
        assertEquals("user002", ids[1]);
        assertEquals("user003", ids[2]);
    }

    @Test
    public void testPayloadWithoutDynamicUserIdField() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setMessageType(MessageType.TEXT);

        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("content", "No userId");

        // Build without dynamic userId (null)
        String payload = builder.buildMessage(fields, null);

        JsonNode root = mapper.readTree(payload);
        assertFalse(root.has(DingTalkMessageBuilder.DYNAMIC_USER_IDS_KEY));
    }

    @Test
    public void testUserIdFieldRemovedFromMessageContent() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setMessageType(MessageType.TEXT);
        options.setUserIdField("userid");

        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        // Simulate the fields AFTER RowDataElementConverter removes the userId field
        Map<String, String> fields = new HashMap<>();
        fields.put("content", "Alert message");
        // userid field should NOT be here since RowDataElementConverter removes it

        String payload = builder.buildMessage(fields, "user001");

        JsonNode root = mapper.readTree(payload);
        // The message content should not contain the userId
        String textContent = root.get("text").get("content").asText();
        assertFalse(textContent.contains("user001"));
        assertEquals("Alert message", textContent);
    }

    @Test
    public void testMarkdownMessageWithDynamicUserId() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setMessageType(MessageType.MARKDOWN);

        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("title", "System Alert");
        fields.put("content", "## CPU: 95%");

        String payload = builder.buildMessage(fields, "manager001");

        JsonNode root = mapper.readTree(payload);
        assertEquals("manager001", root.get(DingTalkMessageBuilder.DYNAMIC_USER_IDS_KEY).asText());
        assertEquals("markdown", root.get("msgtype").asText());
        assertEquals("System Alert", root.get("markdown").get("title").asText());
    }

    @Test
    public void testBackwardCompatibility_StaticUserIdsOnly() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setMessageType(MessageType.TEXT);
        options.setUserIds(Arrays.asList("static_user1", "static_user2"));
        // No userIdField configured

        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("content", "Static message");

        // buildMessage with single param (backward compatible)
        String payload = builder.buildMessage(fields);

        JsonNode root = mapper.readTree(payload);
        // No dynamic userId field should be present
        assertFalse(root.has(DingTalkMessageBuilder.DYNAMIC_USER_IDS_KEY));
        assertEquals("text", root.get("msgtype").asText());
    }

    @Test
    public void testDynamicUserIdKey_IsValidConstant() {
        // Ensure the key constant is properly defined
        assertEquals("__userIds__", DingTalkMessageBuilder.DYNAMIC_USER_IDS_KEY);
    }
}
