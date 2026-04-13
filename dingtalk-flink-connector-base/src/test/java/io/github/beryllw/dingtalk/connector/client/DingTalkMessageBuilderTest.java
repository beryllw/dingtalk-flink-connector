package io.github.beryllw.dingtalk.connector.client;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DingTalkMessageBuilder.
 */
public class DingTalkMessageBuilderTest {

    private DingTalkSinkOptions options;

    @BeforeEach
    public void setUp() {
        options = new DingTalkSinkOptions();
    }

    @Test
    public void testBuildTextMessage() {
        options.setMessageType(MessageType.TEXT);
        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("content", "Hello World");

        String result = builder.buildMessage(fields);

        assertNotNull(result);
        assertTrue(result.contains("\"msgtype\":\"text\""));
        assertTrue(result.contains("\"content\":\"Hello World\""));
    }

    @Test
    public void testBuildTextMessageWithAtMobiles() {
        options.setMessageType(MessageType.TEXT);
        options.setAtMobiles(Arrays.asList("13800138000", "13900139000"));
        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("content", "Hello @someone");

        String result = builder.buildMessage(fields);

        assertTrue(result.contains("\"msgtype\":\"text\""));
        assertTrue(result.contains("\"atMobiles\""));
        assertTrue(result.contains("13800138000"));
    }

    @Test
    public void testBuildTextMessageWithAtAll() {
        options.setMessageType(MessageType.TEXT);
        options.setAtAll(true);
        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("content", "Hello @all");

        String result = builder.buildMessage(fields);

        assertTrue(result.contains("\"isAtAll\":true"));
    }

    @Test
    public void testBuildMarkdownMessage() {
        options.setMessageType(MessageType.MARKDOWN);
        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("title", "Alert");
        fields.put("content", "## CPU Usage\n**CPU: 95%**");

        String result = builder.buildMessage(fields);

        assertTrue(result.contains("\"msgtype\":\"markdown\""));
        assertTrue(result.contains("\"title\":\"Alert\""));
        assertTrue(result.contains("\"text\":\"## CPU Usage\\n**CPU: 95%**\""));
    }

    @Test
    public void testBuildActionCardMessage() {
        options.setMessageType(MessageType.ACTION_CARD);
        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("title", "Task Complete");
        fields.put("content", "Job finished successfully.");
        fields.put("singleTitle", "View Log");
        fields.put("actionUrl", "https://example.com/logs");

        String result = builder.buildMessage(fields);

        assertTrue(result.contains("\"msgtype\":\"actionCard\""));
        assertTrue(result.contains("\"title\":\"Task Complete\""));
        assertTrue(result.contains("\"btnOrientation\":\"0\""));
        assertTrue(result.contains("\"actionURL\":\"https://example.com/logs\""));
    }

    @Test
    public void testBuildLinkMessage() {
        options.setMessageType(MessageType.LINK);
        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("title", "New Article");
        fields.put("content", "Check this out!");
        fields.put("messageUrl", "https://example.com/article");
        fields.put("picUrl", "https://example.com/image.png");

        String result = builder.buildMessage(fields);

        assertTrue(result.contains("\"msgtype\":\"link\""));
        assertTrue(result.contains("\"messageUrl\":\"https://example.com/article\""));
        assertTrue(result.contains("\"picUrl\":\"https://example.com/image.png\""));
    }

    @Test
    public void testTextFieldFallbackToContent() {
        options.setMessageType(MessageType.TEXT);
        DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

        Map<String, String> fields = new HashMap<>();
        fields.put("text", "Fallback content");

        String result = builder.buildMessage(fields);

        assertTrue(result.contains("\"content\":\"Fallback content\""));
    }
}
