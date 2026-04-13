package io.github.beryllw.dingtalk.connector.config;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration options for the DingTalk Sink.
 */
public class DingTalkSinkOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    // Webhook mode options
    private String webhook;
    private String secret;

    // API mode options
    private String appKey;
    private String appSecret;
    private String robotCode;
    private List<String> userIds;

    // Common options
    private SendMode sendMode = SendMode.WEBHOOK;
    private MessageType messageType = MessageType.TEXT;
    private List<String> atMobiles;
    private boolean atAll = false;
    private String userIdField;
    private int maxRetries = 3;
    private long retryDelayMs = 1000;
    private int batchSize = 1;
    private int connectionTimeoutMs = 5000;

    public DingTalkSinkOptions() {
    }

    // --- Webhook ---

    public String getWebhook() {
        return webhook;
    }

    public void setWebhook(String webhook) {
        this.webhook = webhook;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    // --- API ---

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getAppSecret() {
        return appSecret;
    }

    public void setAppSecret(String appSecret) {
        this.appSecret = appSecret;
    }

    public String getRobotCode() {
        return robotCode;
    }

    public void setRobotCode(String robotCode) {
        this.robotCode = robotCode;
    }

    public List<String> getUserIds() {
        return userIds;
    }

    public void setUserIds(List<String> userIds) {
        this.userIds = userIds;
    }

    // --- Common ---

    public SendMode getSendMode() {
        return sendMode;
    }

    public void setSendMode(SendMode sendMode) {
        this.sendMode = sendMode;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public List<String> getAtMobiles() {
        return atMobiles;
    }

    public void setAtMobiles(List<String> atMobiles) {
        this.atMobiles = atMobiles;
    }

    public boolean isAtAll() {
        return atAll;
    }

    public void setAtAll(boolean atAll) {
        this.atAll = atAll;
    }

    public String getUserIdField() {
        return userIdField;
    }

    public void setUserIdField(String userIdField) {
        this.userIdField = userIdField;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public void setRetryDelayMs(long retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    @Override
    public String toString() {
        return "DingTalkSinkOptions{" +
                "sendMode=" + sendMode +
                ", messageType=" + messageType +
                ", batchSize=" + batchSize +
                ", maxRetries=" + maxRetries +
                '}';
    }
}
