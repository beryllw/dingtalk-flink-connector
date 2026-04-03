package io.github.beryllw.dingtalk.connector.sink;

import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DataStream API integration test using MiniCluster.
 */
public class DingTalkSinkDataStreamITCase {

    private static final int NUM_RECORDS = 10;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    private HttpServer mockServer;
    private final List<String> receivedMessages = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        mockServer = HttpServer.create(new InetSocketAddress(0), 0);
        mockServer.createContext("/dingtalk", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String message = new String(body, StandardCharsets.UTF_8);
            receivedMessages.add(message);

            String response = "{\"errcode\":0,\"errmsg\":\"ok\"}";
            exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
            exchange.close();
        });
        mockServer.start();
    }

    @AfterEach
    void tearDown() {
        if (mockServer != null) {
            mockServer.stop(0);
        }
    }

    @Test
    void testSendTextMessages() throws Exception {
        String webhook = "http://localhost:" + mockServer.getAddress().getPort() + "/dingtalk";

        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setWebhook(webhook);
        options.setMessageType(MessageType.TEXT);
        options.setMaxRetries(0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> data = IntStream.range(0, NUM_RECORDS)
                .mapToObj(i -> "test message " + i)
                .collect(Collectors.toList());

        env.fromCollection(data, Types.STRING)
                .sinkTo(DingTalkSink.<String>builder()
                        .setOptions(options)
                        .setMaxBatchSize(1)
                        .setMaxTimeInBufferMS(100)
                        .build());

        env.execute("DingTalk DataStream Text Test");

        assertEquals(NUM_RECORDS, receivedMessages.size());

        for (int i = 0; i < NUM_RECORDS; i++) {
            String msg = receivedMessages.get(i);
            assertTrue(msg.contains("\"msgtype\":\"text\""));
            assertTrue(msg.contains("\"content\":\"test message " + i + "\""));
        }
    }

    @Test
    void testSendMarkdownMessages() throws Exception {
        String webhook = "http://localhost:" + mockServer.getAddress().getPort() + "/dingtalk";

        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setWebhook(webhook);
        options.setMessageType(MessageType.MARKDOWN);
        options.setMaxRetries(0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Map<String, String>> data = IntStream.range(0, NUM_RECORDS)
                .mapToObj(i -> Map.of(
                        "title", "Alert " + i,
                        "content", "## Alert " + i + "\n**CPU: " + (80 + i) + "%**"))
                .collect(Collectors.toList());

        env.fromCollection(data, Types.MAP(Types.STRING, Types.STRING))
                .sinkTo(DingTalkSink.<Map<String, String>>builder()
                        .setOptions(options)
                        .setMaxBatchSize(1)
                        .setMaxTimeInBufferMS(100)
                        .build());

        env.execute("DingTalk DataStream Markdown Test");

        assertEquals(NUM_RECORDS, receivedMessages.size());

        for (String msg : receivedMessages) {
            assertTrue(msg.contains("\"msgtype\":\"markdown\""));
            assertTrue(msg.contains("\"title\":"));
            assertTrue(msg.contains("\"text\":"));
        }
    }

    @Test
    void testWebhookWithSigningSecret() throws Exception {
        String webhook = "http://localhost:" + mockServer.getAddress().getPort() + "/dingtalk?access_token=test";

        DingTalkSinkOptions options = new DingTalkSinkOptions();
        options.setWebhook(webhook);
        options.setSecret("test-secret-key");
        options.setMessageType(MessageType.TEXT);
        options.setMaxRetries(0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> data = IntStream.range(0, 3)
                .mapToObj(i -> "signed message " + i)
                .collect(Collectors.toList());

        env.fromCollection(data, Types.STRING)
                .sinkTo(DingTalkSink.<String>builder()
                        .setOptions(options)
                        .setMaxBatchSize(1)
                        .setMaxTimeInBufferMS(100)
                        .build());

        env.execute("DingTalk DataStream Signed Webhook Test");

        assertEquals(3, receivedMessages.size());

        for (String msg : receivedMessages) {
            assertTrue(msg.contains("\"msgtype\":\"text\""));
        }
    }
}
