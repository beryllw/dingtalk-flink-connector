package io.github.beryllw.dingtalk.connector.client;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import io.github.beryllw.dingtalk.connector.config.SendMode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DingTalk Webhook client.
 *
 * <p>Requires the following environment variables:
 * <ul>
 *   <li>DINGTALK_WEBHOOK - Webhook URL (with access_token)</li>
 *   <li>DINGTALK_SECRET (optional) - Webhook signing secret</li>
 * </ul>
 *
 * <p>Set them before running (source .env or export):
 * <pre>
 * source .env
 * mvn test -Dtest=DingTalkWebhookClientIntegrationTest
 * </pre>
 */
public class DingTalkWebhookClientIntegrationTest {

    private String webhook;
    private String secret;

    @BeforeEach
    public void setUp() {
        webhook = System.getenv("DINGTALK_WEBHOOK");
        secret = System.getenv("DINGTALK_SECRET");

        Assumptions.assumeTrue(webhook != null && !webhook.isEmpty(), "DINGTALK_WEBHOOK not set");
    }

    @Test
    public void testSendTextMessage() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.TEXT);
        DingTalkWebhookClient client = new DingTalkWebhookClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            Map<String, String> fields = new HashMap<>();
            fields.put("content", "[测试] Webhook 集成测试 - 文本消息");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);
            System.out.println("Webhook text response: " + response);
            assertTrue(response.contains("\"errcode\":0") || response.contains("\"errmsg\":\"ok\""),
                    "Response should indicate success: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSendMarkdownMessage() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.MARKDOWN);
        DingTalkWebhookClient client = new DingTalkWebhookClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            Map<String, String> fields = new HashMap<>();
            fields.put("title", "Webhook 测试");
            fields.put("content", "## Webhook 集成测试\n\n- 类型: Markdown\n- 状态: 通过");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);
            System.out.println("Webhook markdown response: " + response);
            assertTrue(response.contains("\"errcode\":0") || response.contains("\"errmsg\":\"ok\""),
                    "Response should indicate success: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testRateLimiting() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.TEXT);
        DingTalkWebhookClient client = new DingTalkWebhookClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

            // Send 5 messages to test rate limiting (webhook limit: 20/min)
            for (int i = 0; i < 5; i++) {
                Map<String, String> fields = new HashMap<>();
                fields.put("content", "[测试] 限流测试消息 " + (i + 1));

                String payload = builder.buildMessage(fields);
                String response = client.send(payload);
                System.out.println("Rate limit test " + (i + 1) + " response: " + response);
            }
        } finally {
            client.close();
        }
    }

    private DingTalkSinkOptions createOptions(MessageType messageType) {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setSendMode(SendMode.WEBHOOK);
        options.setWebhook(webhook);
        options.setSecret(secret);
        options.setMessageType(messageType);
        return options;
    }
}
