package com.example.flink.connector.dingtalk.client;

import com.example.flink.connector.dingtalk.config.DingTalkSinkOptions;
import com.example.flink.connector.dingtalk.config.MessageType;
import com.example.flink.connector.dingtalk.config.SendMode;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
 * <p>Set them before running:
 * <pre>
 * export DINGTALK_APP_KEY=your-app-key
 * export DINGTALK_APP_SECRET=your-app-secret
 * export DINGTALK_ROBOT_CODE=your-robot-code
 * export DINGTALK_USER_ID=manager123
 * mvn test -Dtest=DingTalkApiClientIntegrationTest
 * </pre>
 */
public class DingTalkApiClientIntegrationTest {

    private String appKey;
    private String appSecret;
    private String robotCode;
    private String userId;

    @Before
    public void setUp() {
        appKey = System.getenv("DINGTALK_APP_KEY");
        appSecret = System.getenv("DINGTALK_APP_SECRET");
        robotCode = System.getenv("DINGTALK_ROBOT_CODE");
        userId = System.getenv("DINGTALK_USER_ID");

        Assume.assumeTrue("DINGTALK_APP_KEY not set", appKey != null && !appKey.isEmpty());
        Assume.assumeTrue("DINGTALK_APP_SECRET not set", appSecret != null && !appSecret.isEmpty());
        Assume.assumeTrue("DINGTALK_ROBOT_CODE not set", robotCode != null && !robotCode.isEmpty());
        Assume.assumeTrue("DINGTALK_USER_ID not set", userId != null && !userId.isEmpty());
    }

    @Test
    public void testSendTextMessage() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setSendMode(SendMode.API);
        options.setAppKey(appKey);
        options.setAppSecret(appSecret);
        options.setRobotCode(robotCode);
        options.setUserIds(Arrays.asList(userId));
        options.setMessageType(MessageType.TEXT);

        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

            Map<String, String> fields = new HashMap<>();
            fields.put("content", "[测试] 这是一条来自 Flink DingTalk Connector 的集成测试消息");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);

            System.out.println("Text message response: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSendMarkdownMessage() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setSendMode(SendMode.API);
        options.setAppKey(appKey);
        options.setAppSecret(appSecret);
        options.setRobotCode(robotCode);
        options.setUserIds(Arrays.asList(userId));
        options.setMessageType(MessageType.MARKDOWN);

        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

            Map<String, String> fields = new HashMap<>();
            fields.put("title", "Flink 告警测试");
            fields.put("content", "## 系统告警\n\n- **CPU 使用率**: 95%\n- **内存使用率**: 80%\n- **磁盘使用率**: 60%");

            String payload = builder.buildMessage(fields);
            String response = client.send(payload);

            System.out.println("Markdown message response: " + response);
        } finally {
            client.close();
        }
    }

    @Test
    public void testSendActionCardMessage() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setSendMode(SendMode.API);
        options.setAppKey(appKey);
        options.setAppSecret(appSecret);
        options.setRobotCode(robotCode);
        options.setUserIds(Arrays.asList(userId));
        options.setMessageType(MessageType.ACTION_CARD);

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
        } finally {
            client.close();
        }
    }

    @Test
    public void testSendLinkMessage() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setSendMode(SendMode.API);
        options.setAppKey(appKey);
        options.setAppSecret(appSecret);
        options.setRobotCode(robotCode);
        options.setUserIds(Arrays.asList(userId));
        options.setMessageType(MessageType.LINK);

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
        } finally {
            client.close();
        }
    }

    @Test
    public void testTokenRefresh() throws Exception {
        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setSendMode(SendMode.API);
        options.setAppKey(appKey);
        options.setAppSecret(appSecret);
        options.setRobotCode(robotCode);
        options.setUserIds(Arrays.asList(userId));
        options.setMessageType(MessageType.TEXT);

        DingTalkApiClient client = new DingTalkApiClient(options);
        try {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);

            for (int i = 0; i < 3; i++) {
                Map<String, String> fields = new HashMap<>();
                fields.put("content", "[测试] 批量测试消息 " + (i + 1));

                String payload = builder.buildMessage(fields);
                String response = client.send(payload);
                System.out.println("Message " + (i + 1) + " response: " + response);
            }
        } finally {
            client.close();
        }
    }
}
