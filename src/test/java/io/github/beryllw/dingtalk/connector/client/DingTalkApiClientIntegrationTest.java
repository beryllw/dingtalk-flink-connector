package io.github.beryllw.dingtalk.connector.client;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import io.github.beryllw.dingtalk.connector.config.SendMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DingTalk API client.
 *
 * <p>Requires the following environment variables:
 * <ul>
 *   <li>DINGTALK_APP_KEY - Enterprise app AppKey</li>
 *   <li>DINGTALK_APP_SECRET - Enterprise app AppSecret</li>
 *   <li>DINGTALK_ROBOT_CODE - Robot code (usually same as AppKey)</li>
 *   <li>DINGTALK_USER_ID - Target user ID to receive messages</li>
 * </ul>
 *
 * <p>Set them before running (source .env or export):
 * <pre>
 * source .env
 * mvn test -Dtest=DingTalkApiClientIntegrationTest
 * </pre>
 */
public class DingTalkApiClientIntegrationTest {

    private String appKey;
    private String appSecret;
    private String robotCode;
    private String userId;

    @BeforeEach
    public void setUp() {
        appKey = System.getenv("DINGTALK_APP_KEY");
        appSecret = System.getenv("DINGTALK_APP_SECRET");
        robotCode = System.getenv("DINGTALK_ROBOT_CODE");
        userId = System.getenv("DINGTALK_USER_ID");

        Assumptions.assumeTrue(appKey != null && !appKey.isEmpty(), "DINGTALK_APP_KEY not set");
        Assumptions.assumeTrue(appSecret != null && !appSecret.isEmpty(), "DINGTALK_APP_SECRET not set");
        Assumptions.assumeTrue(robotCode != null && !robotCode.isEmpty(), "DINGTALK_ROBOT_CODE not set");
        Assumptions.assumeTrue(userId != null && !userId.isEmpty(), "DINGTALK_USER_ID not set");
    }

    @Test
    public void testSendTextMessage() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.TEXT);
        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            Map<String, String> fields = new HashMap<>();
            fields.put("content", "[测试] 这是一条来自 Flink DingTalk Connector 的集成测试消息");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);
            System.out.println("Text message response: " + response);
            assertTrue(response.contains("\"errcode\":0") || response.contains("\"errmsg\":\"ok\""),
                    "Response should indicate success: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSendMarkdownMessage() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.MARKDOWN);
        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            Map<String, String> fields = new HashMap<>();
            fields.put("title", "Flink 告警测试");
            fields.put("content", "## 系统告警\n\n- **CPU 使用率**: 95%\n- **内存使用率**: 80%\n- **磁盘使用率**: 60%");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);
            System.out.println("Markdown message response: " + response);
            assertTrue(response.contains("\"errcode\":0") || response.contains("\"errmsg\":\"ok\""),
                    "Response should indicate success: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSendActionCardMessage() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.ACTION_CARD);
        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            Map<String, String> fields = new HashMap<>();
            fields.put("title", "任务完成通知");
            fields.put("content", "## Flink 作业已完成\n\n作业 ID: job-001\n运行时长: 2h 30m\n处理记录数: 1,000,000");
            fields.put("singleTitle", "查看详情");
            fields.put("actionUrl", "https://flink.apache.org");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);
            System.out.println("ActionCard message response: " + response);
            assertTrue(response.contains("\"errcode\":0") || response.contains("\"errmsg\":\"ok\""),
                    "Response should indicate success: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSendLinkMessage() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.LINK);
        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            Map<String, String> fields = new HashMap<>();
            fields.put("title", "Flink 1.20 发布");
            fields.put("content", "Apache Flink 1.20 已正式发布，包含多项重要更新。");
            fields.put("messageUrl", "https://flink.apache.org/news/");
            fields.put("picUrl", "");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);
            System.out.println("Link message response: " + response);
            assertTrue(response.contains("\"errcode\":0") || response.contains("\"errmsg\":\"ok\""),
                    "Response should indicate success: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testTokenRefresh() throws Exception {
        DingTalkSinkOptions options = createOptions(MessageType.TEXT);
        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            for (int i = 0; i < 3; i++) {
                Map<String, String> fields = new HashMap<>();
                fields.put("content", "[测试] 批量测试消息 " + (i + 1));

                String payload = builder.buildMessage(fields);
                String response = client.send(payload);
                System.out.println("Message " + (i + 1) + " response: " + response);
                assertTrue(response.contains("\"errcode\":0") || response.contains("\"errmsg\":\"ok\""),
                        "Message " + (i + 1) + " should succeed: " + response);
            }
        } finally {
            client.close();
        }
    }

    private DingTalkSinkOptions createOptions(MessageType messageType) {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setSendMode(SendMode.API);
        options.setAppKey(appKey);
        options.setAppSecret(appSecret);
        options.setRobotCode(robotCode);
        options.setUserIds(Arrays.asList(userId));
        options.setMessageType(messageType);
        return options;
    }
}
