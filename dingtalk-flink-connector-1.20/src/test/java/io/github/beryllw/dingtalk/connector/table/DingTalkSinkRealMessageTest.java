package io.github.beryllw.dingtalk.connector.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Send a real DingTalk message via SQL.
 * Run with: mvn test -Dtest=DingTalkSinkRealMessageTest
 *
 * Credentials are loaded from .env file in project root.
 * Source the file before running: source .env
 * Or run: export DINGTALK_APP_KEY=xxx DINGTALK_APP_SECRET=xxx DINGTALK_ROBOT_CODE=xxx DINGTALK_USER_ID=xxx
 */
public class DingTalkSinkRealMessageTest {

    private static boolean hasCredentials() {
        return System.getenv("DINGTALK_APP_KEY") != null
                && System.getenv("DINGTALK_APP_SECRET") != null
                && System.getenv("DINGTALK_ROBOT_CODE") != null
                && System.getenv("DINGTALK_USER_ID") != null;
    }

    @Test
    @EnabledIf("hasCredentials")
    void testSendRealMessageViaSQL() throws Exception {
        String appKey = System.getenv("DINGTALK_APP_KEY");
        String appSecret = System.getenv("DINGTALK_APP_SECRET");
        String robotCode = System.getenv("DINGTALK_ROBOT_CODE");
        String userId = System.getenv("DINGTALK_USER_ID");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // Source: generate one row using sequence
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

        // Sink: DingTalk API
        tableEnv.executeSql("CREATE TEMPORARY TABLE dingtalk_sink (" +
                "content STRING," +
                "title STRING" +
                ") WITH (" +
                "  'connector' = 'dingtalk'," +
                "  'send-mode' = 'api'," +
                "  'app-key' = '" + appKey + "'," +
                "  'app-secret' = '" + appSecret + "'," +
                "  'robot-code' = '" + robotCode + "'," +
                "  'user-ids' = '" + userId + "'," +
                "  'message-type' = 'text'" +
                ")");

        TableResult result = tableEnv.executeSql(
                "INSERT INTO dingtalk_sink SELECT content, title FROM source_table");

        result.await();
        assertNotNull(result.getJobClient().orElseThrow(), "Job client should not be null");
        System.out.println("Message sent successfully!");
    }
}
