package com.example.flink.connector.dingtalk.client;

import com.example.flink.connector.dingtalk.config.DingTalkSinkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * DingTalk Enterprise Application API client with access_token caching.
 * Uses the batchSend endpoint for sending single-chat robot messages.
 *
 * <p>API format:
 * <pre>
 * POST /v1.0/robot/oToMessages/batchSend
 * {
 *   "robotCode": "xxx",
 *   "userIds": ["user1"],
 *   "msgKey": "sampleText",
 *   "msgParam": "{\"content\":\"hello\"}"
 * }
 * </pre>
 */
public class DingTalkApiClient implements DingTalkClient {

    private static final Logger LOG = LoggerFactory.getLogger(DingTalkApiClient.class);
    private static final String TOKEN_URL = "https://api.dingtalk.com/v1.0/oauth2/accessToken";
    private static final String ROBOT_SEND_URL = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend";

    private static final long TOKEN_REFRESH_BUFFER_MS = 300_000L; // 300 seconds buffer

    private final String appKey;
    private final String appSecret;
    private final String robotCode;
    private final List<String> userIds;
    private final int connectionTimeoutMs;

    private volatile String accessToken;
    private volatile long tokenExpireTime;

    public DingTalkApiClient(DingTalkSinkOptions options) {
        this.appKey = options.getAppKey();
        this.appSecret = options.getAppSecret();
        this.robotCode = options.getRobotCode();
        this.userIds = options.getUserIds();
        this.connectionTimeoutMs = options.getConnectionTimeoutMs();
    }

    @Override
    public synchronized String send(String payload) throws IOException {
        ensureValidToken();

        ObjectMapper mapper = new ObjectMapper();
        String msgKey = extractMsgKey(payload);
        String msgParam = extractMsgParam(payload);

        ObjectNode body = mapper.createObjectNode();
        body.put("robotCode", robotCode);
        ArrayNode uidArray = body.putArray("userIds");
        for (String uid : userIds) {
            uidArray.add(uid);
        }
        body.put("msgKey", msgKey);
        body.put("msgParam", msgParam);

        String bodyStr = mapper.writeValueAsString(body);

        LOG.debug("DingTalk API request: {}", bodyStr);

        return doPost(ROBOT_SEND_URL, bodyStr, accessToken);
    }

    @Override
    public void close() {
        // No resources to release
    }

    private synchronized void ensureValidToken() throws IOException {
        long now = System.currentTimeMillis();
        if (accessToken == null || now > tokenExpireTime - TOKEN_REFRESH_BUFFER_MS) {
            refreshToken();
        }
    }

    private void refreshToken() throws IOException {
        String body = "{\"appKey\":\"" + appKey + "\",\"appSecret\":\"" + appSecret + "\"}";
        String response = doPost(TOKEN_URL, body, null);

        int idx = response.indexOf("\"accessToken\"");
        if (idx < 0) {
            throw new IOException("Failed to get access token: " + response);
        }

        int start = response.indexOf('"', idx + 13) + 1;
        int end = response.indexOf('"', start);
        String token = response.substring(start, end);

        int expireIdx = response.indexOf("\"expireIn\"");
        long expireIn = 7200;
        if (expireIdx >= 0) {
            int colonIdx = response.indexOf(':', expireIdx);
            int numStart = colonIdx + 1;
            int numEnd = response.indexOf(',', numStart);
            if (numEnd < 0) {
                numEnd = response.indexOf('}', numStart);
            }
            expireIn = Long.parseLong(response.substring(numStart, numEnd).trim());
        }

        this.accessToken = token;
        this.tokenExpireTime = System.currentTimeMillis() + expireIn * 1000L;
        LOG.info("DingTalk access token refreshed, expires in {} seconds", expireIn);
    }

    /**
     * Map message type to DingTalk msgKey.
     */
    private String extractMsgKey(String payload) {
        if (payload.contains("\"msgtype\":\"markdown\"")) return "sampleMarkdown";
        if (payload.contains("\"msgtype\":\"actionCard\"")) return "sampleActionCard";
        if (payload.contains("\"msgtype\":\"link\"")) return "sampleLink";
        return "sampleText";
    }

    /**
     * Extract the inner message object as msgParam JSON string (escaped).
     * The API expects msgParam to be a JSON-encoded string, not an object.
     */
    private String extractMsgParam(String payload) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(payload);
            String msgType = root.path("msgtype").asText();
            JsonNode msgNode = root.path(msgType);
            // Return as escaped JSON string
            return mapper.writeValueAsString(msgNode);
        } catch (Exception e) {
            return "{\"content\":\"\"}";
        }
    }

    private String doPost(String urlString, String payload, String token) throws IOException {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(connectionTimeoutMs);
        conn.setReadTimeout(connectionTimeoutMs);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
        if (token != null) {
            conn.setRequestProperty("x-acs-dingtalk-access-token", token);
        }

        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        String response;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        responseCode == 200 ? conn.getInputStream() : conn.getErrorStream(),
                        StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            response = sb.toString();
        }

        if (responseCode != 200) {
            if (response.contains("40014") || response.contains("42001") || response.contains("invalid token")) {
                LOG.warn("Token expired or invalid, refreshing...");
                synchronized (this) {
                    refreshToken();
                }
                return doPost(urlString, payload, accessToken);
            }
            throw new IOException("DingTalk API request failed with code " + responseCode + ": " + response);
        }

        LOG.debug("DingTalk API response: {}", response);
        return response;
    }
}
