package io.github.beryllw.dingtalk.connector.table;

import com.sun.net.httpserver.HttpServer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL API integration test using MiniCluster.
 */
public class DingTalkSinkTableITCase {

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

    private StreamTableEnvironment createTableEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    @Test
    void testSqlInsertTextMessage() throws Exception {
        String webhook = "http://localhost:" + mockServer.getAddress().getPort() + "/dingtalk";

        StreamTableEnvironment tableEnv = createTableEnv();

        tableEnv.executeSql("CREATE TEMPORARY TABLE source_table (" +
                "content STRING," +
                "title STRING" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'fields.content.kind' = 'sequence'," +
                "  'fields.content.start' = '1'," +
                "  'fields.content.end' = '1'," +
                "  'fields.title.kind' = 'sequence'," +
                "  'fields.title.start' = '1'," +
                "  'fields.title.end' = '1'" +
                ")");

        tableEnv.executeSql("CREATE TEMPORARY TABLE dingtalk_sink (" +
                "content STRING," +
                "title STRING" +
                ") WITH (" +
                "  'connector' = 'dingtalk'," +
                "  'send-mode' = 'webhook'," +
                "  'webhook' = '" + webhook + "'," +
                "  'message-type' = 'text'" +
                ")");

        TableResult result = tableEnv.executeSql(
                "INSERT INTO dingtalk_sink SELECT content, title FROM source_table");

        result.await();

        assertEquals(1, receivedMessages.size());

        String msg = receivedMessages.get(0);
        assertTrue(msg.contains("\"msgtype\":\"text\""), "Received: " + msg);
        assertTrue(msg.contains("\"content\":\"1\""), "Received: " + msg);
    }

    @Test
    void testSqlInsertMarkdownMessage() throws Exception {
        String webhook = "http://localhost:" + mockServer.getAddress().getPort() + "/dingtalk";

        StreamTableEnvironment tableEnv = createTableEnv();

        tableEnv.executeSql("CREATE TEMPORARY TABLE source_table (" +
                "content STRING," +
                "title STRING" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'fields.content.kind' = 'sequence'," +
                "  'fields.content.start' = '1'," +
                "  'fields.content.end' = '1'," +
                "  'fields.title.kind' = 'sequence'," +
                "  'fields.title.start' = '1'," +
                "  'fields.title.end' = '1'" +
                ")");

        tableEnv.executeSql("CREATE TEMPORARY TABLE dingtalk_sink (" +
                "content STRING," +
                "title STRING" +
                ") WITH (" +
                "  'connector' = 'dingtalk'," +
                "  'send-mode' = 'webhook'," +
                "  'webhook' = '" + webhook + "'," +
                "  'message-type' = 'markdown'" +
                ")");

        TableResult result = tableEnv.executeSql(
                "INSERT INTO dingtalk_sink SELECT content, title FROM source_table");

        result.await();

        assertEquals(1, receivedMessages.size());

        String msg = receivedMessages.get(0);
        assertTrue(msg.contains("\"msgtype\":\"markdown\""));
        assertTrue(msg.contains("\"title\":\"1\""));
    }

    @Test
    void testSqlWithAtMobiles() throws Exception {
        String webhook = "http://localhost:" + mockServer.getAddress().getPort() + "/dingtalk";

        StreamTableEnvironment tableEnv = createTableEnv();

        tableEnv.executeSql("CREATE TEMPORARY TABLE source_table (" +
                "content STRING" +
                ") WITH (" +
                "  'connector' = 'datagen'," +
                "  'fields.content.kind' = 'sequence'," +
                "  'fields.content.start' = '1'," +
                "  'fields.content.end' = '1'" +
                ")");

        tableEnv.executeSql("CREATE TEMPORARY TABLE dingtalk_sink (" +
                "content STRING" +
                ") WITH (" +
                "  'connector' = 'dingtalk'," +
                "  'send-mode' = 'webhook'," +
                "  'webhook' = '" + webhook + "'," +
                "  'message-type' = 'text'," +
                "  'at-mobiles' = '13800138000,13900139000'" +
                ")");

        TableResult result = tableEnv.executeSql(
                "INSERT INTO dingtalk_sink SELECT content FROM source_table");

        result.await();

        assertEquals(1, receivedMessages.size());

        String msg = receivedMessages.get(0);
        assertTrue(msg.contains("\"msgtype\":\"text\""));
        assertTrue(msg.contains("\"atMobiles\""));
        assertTrue(msg.contains("13800138000"));
        assertTrue(msg.contains("13900139000"));
    }
}
