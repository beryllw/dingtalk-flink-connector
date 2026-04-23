# Flink DingTalk Sink Connector - User Manual

**Version:** 1.0.0-SNAPSHOT | **Flink:** 1.20 / 2.2 | **Java:** 11+

[English](USER_GUIDE.md) | [中文](USER_GUIDE_zh.md)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Installation](#3-installation)
4. [Getting Credentials](#4-getting-credentials)
5. [SQL API Usage](#5-sql-api-usage)
6. [DataStream API Usage](#6-datastream-api-usage)
7. [Message Types](#7-message-types)
8. [Configuration Reference](#8-configuration-reference)
9. [Advanced Features](#9-advanced-features)
10. [Testing](#10-testing)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Overview

The Flink DingTalk Sink Connector sends Flink stream data as DingTalk messages. It supports two sending modes:

| Mode | Use Case | Description |
|------|----------|-------------|
| **Webhook** | Group notifications | Sends to a group chat via a robot webhook URL |
| **Enterprise API** | Single chat / targeted | Sends to specific users via the DingTalk enterprise API |

### Supported Message Types

| Type | Description | Required Fields |
|------|-------------|-----------------|
| `text` | Plain text message | `content` |
| `markdown` | Rich text with Markdown formatting | `title`, `content` |
| `actionCard` | Card with action buttons | `title`, `content`, optionally `actionUrl`, `singleTitle` |
| `link` | Message with a clickable link | `title`, `content`, `messageUrl`, optionally `picUrl` |

---

## 2. Architecture

```
+----------------+     +------------------+     +------------------+
|   Flink Source |---->|  DingTalkSink    |---->|  DingTalk Server |
|   (DataStream) |     |  (SinkV2 API)    |     |  (Webhook/API)   |
+----------------+     +------------------+     +------------------+
                              |
                              v
                       +------------------+
                       | Message Builder  |
                       | (Jackson JSON)   |
                       +------------------+
                              |
                              v
                       +------------------+
                       | DingTalk Client  |
                       | (Webhook/API)    |
                       +------------------+
```

The connector uses Flink's **AsyncSinkBase** for non-blocking batch sending with configurable buffering, batching, and retry logic.

---

## 3. Installation

### 3.1 Build from Source

```bash
git clone <repo-url>
cd dingtalk-flink-connector
mvn clean package -DskipTests
```

The shaded JARs will be at:
- `dingtalk-flink-connector-1.20/target/dingtalk-flink-connector-1.20-1.0.0-SNAPSHOT.jar` (for Flink 1.20)
- `dingtalk-flink-connector-2.2/target/dingtalk-flink-connector-2.2-1.0.0-SNAPSHOT.jar` (for Flink 2.2)

### 3.2 Deploy to Flink Cluster

Copy the JAR to Flink's `lib/` directory:

```bash
# For Flink 1.20
cp dingtalk-flink-connector-1.20/target/dingtalk-flink-connector-1.20-1.0.0-SNAPSHOT.jar $FLINK_HOME/lib/

# For Flink 2.2
cp dingtalk-flink-connector-2.2/target/dingtalk-flink-connector-2.2-1.0.0-SNAPSHOT.jar $FLINK_HOME/lib/
```

Or submit with the job:

```bash
flink run -c com.example.YourJob \
  -C file:///path/to/dingtalk-flink-connector-1.20-1.0.0-SNAPSHOT.jar \
  your-job.jar
```

### 3.3 Maven Dependency

```xml
<!-- For Flink 1.20 -->
<dependency>
    <groupId>io.github.beryllw</groupId>
    <artifactId>dingtalk-flink-connector-1.20</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>

<!-- For Flink 2.2 -->
<dependency>
    <groupId>io.github.beryllw</groupId>
    <artifactId>dingtalk-flink-connector-2.2</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## 4. Getting Credentials

### 4.1 Webhook Mode (Group Robot)

1. Open DingTalk group chat → **Settings** → **Intelligent Assistant** → **Add Robot**
2. Select **Custom** robot
3. Set security to **Sign** and copy the **Secret**
4. Copy the **Webhook URL**

You will get:
- `webhook`: `https://oapi.dingtalk.com/robot/send?access_token=xxx`
- `secret`: `SECxxx...`

### 4.2 Enterprise API Mode (Single Chat)

1. Go to [DingTalk Open Platform](https://open-dev.dingtalk.com/)
2. Create an enterprise application with **Robot** capability enabled
3. In app settings, note:
   - **AppKey** (Client ID)
   - **AppSecret** (Client Secret)
   - **RobotCode** (the robot identifier, starts with `ding`)
4. Get the target user's **UserID** from DingTalk admin console or API

You will get:
- `app-key`: `dingxxxxxxxxx`
- `app-secret`: `xxxxxxxxxxxxxxxxxxxx`
- `robot-code`: `dingxxxxxxxxx`
- `user-ids`: Comma-separated list of user IDs

---

## 5. SQL API Usage

### 5.1 Basic Text Message (Webhook)

```sql
CREATE TABLE dingtalk_sink (
    content STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'webhook',
    'webhook' = 'https://oapi.dingtalk.com/robot/send?access_token=xxx',
    'secret' = 'SECxxx',
    'message-type' = 'text'
);

INSERT INTO dingtalk_sink
SELECT 'System alert: CPU usage exceeds 90%' FROM alert_stream;
```

### 5.2 Markdown Message with @ Mention

```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'webhook',
    'webhook' = 'https://oapi.dingtalk.com/robot/send?access_token=xxx',
    'message-type' = 'markdown',
    'at-mobiles' = '13800138000,13900139000'
);

INSERT INTO dingtalk_sink
SELECT
    '## Service Alert\n**Service:** order-service\n**Error Rate:** 15%\n**Time:** ' || CURRENT_TIMESTAMP,
    'Service Alert'
FROM error_stream;
```

### 5.3 Enterprise API - Single Chat

```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'api',
    'app-key' = 'dingxxxxxxxxx',
    'app-secret' = 'xxxxxxxxxxxxxxxxxxxx',
    'robot-code' = 'dingxxxxxxxxx',
    'user-ids' = 'user001',
    'message-type' = 'markdown'
);

INSERT INTO dingtalk_sink
SELECT content, title FROM processed_stream;
```

### 5.4 Complete Example with All Options

```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'webhook',
    'webhook' = 'https://oapi.dingtalk.com/robot/send?access_token=xxx',
    'message-type' = 'markdown',
    'at-all' = 'false',
    'at-mobiles' = '13800138000',
    'max-retries' = '3',
    'retry-delay-ms' = '1000'
);
```

---

## 6. DataStream API Usage

### 6.1 Basic Usage

```java
import io.github.beryllw.dingtalk.connector.config.DingTalkSinkOptions;
import io.github.beryllw.dingtalk.connector.config.MessageType;
import io.github.beryllw.dingtalk.connector.sink.DingTalkSink;

DingTalkSinkOptions options = new DingTalkSinkOptions();
options.setWebhook("https://oapi.dingtalk.com/robot/send?access_token=xxx");
options.setMessageType(MessageType.TEXT);
options.setMaxRetries(3);

DingTalkSink<String> sink = DingTalkSink.<String>builder()
        .setOptions(options)
        .build();

stream.sinkTo(sink);
```

### 6.2 Builder Shortcuts

```java
stream.sinkTo(DingTalkSink.<String>builder()
        .setWebhook("https://oapi.dingtalk.com/robot/send?access_token=xxx")
        .setSecret("SECxxx")
        .setMessageType(MessageType.TEXT)
        .build());
```

### 6.3 Enterprise API Mode

```java
DingTalkSinkOptions options = new DingTalkSinkOptions();
options.setSendMode(SendMode.API);
options.setAppKey("dingxxxxxxxxx");
options.setAppSecret("xxxxxxxxxxxxxxxxxxxx");
options.setRobotCode("dingxxxxxxxxx");
options.setUserIds(Arrays.asList("user001", "user002"));
options.setMessageType(MessageType.MARKDOWN);

stream.sinkTo(DingTalkSink.<Map<String, String>>builder()
        .setOptions(options)
        .build());
```

### 6.4 Custom Element Converter

```java
stream.sinkTo(DingTalkSink.<MyEvent>builder()
        .setOptions(options)
        .setElementConverter((event, context) -> {
            DingTalkMessageBuilder builder = new DingTalkMessageBuilder(options);
            return builder.buildMessage(Map.of(
                "title", event.getTitle(),
                "content", event.getContent()
            ));
        })
        .build());
```

### 6.5 Performance Tuning

```java
stream.sinkTo(DingTalkSink.<String>builder()
        .setOptions(options)
        .setMaxBatchSize(100)          // Max messages per batch
        .setMaxTimeInBufferMS(1000)    // Flush every 1 second
        .setMaxInFlightRequests(50)    // Max concurrent HTTP requests
        .setMaxBufferedRequests(10000) // Max buffered messages
        .build());
```

---

## 7. Message Types

### 7.1 Text

```sql
-- SQL
INSERT INTO dingtalk_sink SELECT 'Hello World' AS content;
```

```java
// DataStream
options.setMessageType(MessageType.TEXT);
stream.sinkTo(DingTalkSink.<String>builder().setOptions(options).build());
```

### 7.2 Markdown

```sql
-- SQL
INSERT INTO dingtalk_sink
SELECT
    '## Alert\n**CPU:** 95%\n**Memory:** 80%' AS content,
    'System Alert' AS title;
```

```java
// DataStream
options.setMessageType(MessageType.MARKDOWN);
stream.map(e -> Map.of(
    "title", "System Alert",
    "content", "## Alert\n**CPU:** 95%"
)).sinkTo(DingTalkSink.<Map<String, String>>builder().setOptions(options).build());
```

### 7.3 ActionCard

```java
options.setMessageType(MessageType.ACTION_CARD);
stream.map(e -> Map.of(
    "title", "Deployment Result",
    "content", "## Deployment Successful\nApp: my-app\nVersion: 1.2.0",
    "singleTitle", "View Details",
    "actionUrl", "https://example.com/deploy/123"
)).sinkTo(DingTalkSink.<Map<String, String>>builder().setOptions(options).build());
```

### 7.4 Link

```java
options.setMessageType(MessageType.LINK);
stream.map(e -> Map.of(
    "title", "New Article Published",
    "content", "Check out the latest blog post about Flink optimization.",
    "messageUrl", "https://example.com/blog/post-123",
    "picUrl", "https://example.com/image.png"
)).sinkTo(DingTalkSink.<Map<String, String>>builder().setOptions(options).build());
```

---

## 8. Configuration Reference

### SQL Table Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `connector` | String | Yes | - | Must be `dingtalk` |
| `send-mode` | String | No | `webhook` | `webhook` or `api` |
| `webhook` | String | Webhook mode | - | Webhook URL with access_token |
| `secret` | String | No | - | HMAC-SHA256 signing secret |
| `app-key` | String | API mode | - | Enterprise app AppKey |
| `app-secret` | String | API mode | - | Enterprise app AppSecret |
| `robot-code` | String | API mode | - | Robot code (starts with `ding`) |
| `user-ids` | String | API mode | - | Comma-separated user IDs |
| `user-id-field` | String | No | - | Column name for dynamic user ID (API mode) |
| `message-type` | String | No | `text` | `text`, `markdown`, `actionCard`, `link` |
| `at-mobiles` | String | No | - | Comma-separated mobile numbers to @ |
| `at-all` | Boolean | No | `false` | Whether to @ everyone |
| `max-retries` | Int | No | `3` | Max retries on failure |
| `retry-delay-ms` | Long | No | `1000` | Base retry delay in ms |
| `sink.batch.max-size` | Int | No | `20` | Max records per batch (tuned for DingTalk ~20 msg/min) |
| `sink.max-in-flight-requests` | Int | No | `1` | Max concurrent async requests |
| `sink.max-buffered-requests` | Int | No | `100` | Max buffered requests before back-pressure |
| `sink.buffer.flush-interval` | Long | No | `5000` | Buffer flush interval in ms |
| `sink.buffer.max-size-in-bytes` | Long | No | `5242880` | Max buffer size in bytes (5 MB) |

### Programmatic Options (DingTalkSinkOptions)

| Method | Type | Description |
|--------|------|-------------|
| `setWebhook(String)` | String | Set webhook URL |
| `setSecret(String)` | String | Set signing secret |
| `setSendMode(SendMode)` | Enum | `WEBHOOK` or `API` |
| `setAppKey(String)` | String | Set AppKey for API mode |
| `setAppSecret(String)` | String | Set AppSecret for API mode |
| `setRobotCode(String)` | String | Set robot code for API mode |
| `setUserIds(List<String>)` | List | Set target user IDs |
| `setUserIdField(String)` | String | Set column name for dynamic user ID (API mode) |
| `setMessageType(MessageType)` | Enum | `TEXT`, `MARKDOWN`, `ACTION_CARD`, `LINK` |
| `setAtMobiles(List<String>)` | List | Mobile numbers to @ |
| `setAtAll(boolean)` | boolean | @ everyone |
| `setMaxRetries(int)` | int | Max retry attempts |
| `setRetryDelayMs(long)` | long | Base retry delay (ms) |
| `setConnectionTimeoutMs(int)` | int | HTTP connection timeout (ms), default `5000` |

---

## 9. Advanced Features

### 9.1 Rate Limiting (Webhook Mode)

DingTalk webhooks are limited to **20 messages per minute**. The connector uses a token bucket algorithm:
- 20 tokens max capacity
- 1 token refilled every 3 seconds
- Requests wait if no tokens available

### 9.2 Token Caching (API Mode)

Enterprise API access tokens expire after 7200 seconds. The connector:
- Caches the token in memory
- Refreshes 300 seconds before expiry
- Auto-retries on `40014` (invalid token) and `42001` (expired token) errors

### 9.3 Retry Logic

Failed requests are retried with exponential backoff:

```
delay = retryDelayMs * 2^attempt
```

Default: 1s, 2s, 4s (3 retries max).

Retriable error codes:
- `42001` - Token expired
- `40014` - Invalid token
- `40001` - Access token not exist
- `45009` - Rate limit exceeded
- `42014` - Invalid parameter

### 9.4 Field Mapping

For SQL tables, column names map directly to message fields:

| Column Name | Used In | Purpose |
|-------------|---------|---------|
| `content` | All types | Main message body |
| `title` | markdown, actionCard, link | Message title |
| `messageUrl` | link | Clickable URL |
| `picUrl` | link | Thumbnail image URL |
| `singleTitle` | actionCard | Button text |
| `actionUrl` | actionCard | Button link URL |

---

## 10. Testing

### 10.1 Unit Tests

```bash
mvn test -Dtest=DingTalkMessageBuilderTest
```

### 10.2 DataStream Integration Tests

```bash
mvn test -Dtest=DingTalkSinkDataStreamITCase
```

Tests the full DataStream pipeline with a mock HTTP server.

### 10.3 SQL Integration Tests

```bash
mvn test -Dtest=DingTalkSinkTableITCase
```

Tests SQL table creation and INSERT with MiniCluster.

### 10.4 Real API Integration Tests

```bash
# Set credentials
source .env

# Run tests
mvn test -Dtest=DingTalkApiClientIntegrationTest
```

---

## 11. Troubleshooting

### Common Issues

| Error | Cause | Solution |
|-------|-------|----------|
| `MissingrobotCode` | API mode without robotCode | Set `robot-code` in table options |
| `MissingmsgParam` | API mode with wrong payload format | Ensure message-type is set correctly |
| `robotCode not exist` | Invalid robot code | Verify robotCode starts with `ding` |
| `40014` / `42001` | Token expired | Connector auto-refreshes, check AppKey/AppSecret |
| `45009` | Rate limit exceeded | Reduce batch size or increase flush interval |
| Connection timeout | Network unreachable | Check firewall rules, verify URL |

### Debug Logging

Enable debug logging in `log4j.properties`:

```properties
log4j.logger.io.github.beryllw.dingtalk.connector=DEBUG
```

### Performance Tips

- Set `sink.batch.max-size=1` for low-latency (immediate delivery)
- Increase `sink.max-in-flight-requests` for high-throughput scenarios
- Use Enterprise API mode for higher throughput (no 20 msg/min limit)
- For webhook mode, batch messages at the Flink level before sending
