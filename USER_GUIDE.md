# Flink DingTalk Sink Connector - User Manual

**Version:** 1.0.0-SNAPSHOT | **Flink:** 2.2.0 | **Java:** 11+

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
cd dingding-flink-connector
mvn clean package -DskipTests
```

The shaded JAR will be at `target/dingtalk-flink-connector-1.0.0-SNAPSHOT.jar`.

### 3.2 Deploy to Flink Cluster

Copy the JAR to Flink's `lib/` directory:

```bash
cp target/dingtalk-flink-connector-1.0.0-SNAPSHOT.jar $FLINK_HOME/lib/
```

Or submit with the job:

```bash
flink run -c com.example.YourJob \
  -C file:///path/to/dingtalk-flink-connector-1.0.0-SNAPSHOT.jar \
  your-job.jar
```

### 3.3 Maven Dependency

```xml
<dependency>
    <groupId>io.github.beryllw</groupId>
    <artifactId>dingtalk-flink-connector</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## 4. Getting Credentials

### 4.1 Webhook Mode (Group Robot)

1. Open DingTalk group chat → **Settings** → **Intelligent Assistant** → **Add Robot**
2. Select **Custom** robot
3. Set security to **Sign** (加签) and copy the **Secret**
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
    'retry-delay-ms' = '1000',
    'sink.batch.max-size' = '1',
    'sink.flush-buffer.timeout' = '5000',
    'sink.requests.max-inflight' = '50'
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
| `message-type` | String | No | `text` | `text`, `markdown`, `actionCard`, `link` |
| `at-mobiles` | String | No | - | Comma-separated mobile numbers to @ |
| `at-all` | Boolean | No | `false` | Whether to @ everyone |
| `max-retries` | Int | No | `3` | Max retries on failure |
| `retry-delay-ms` | Long | No | `1000` | Base retry delay in ms |
| `sink.batch.max-size` | Int | No | `500` | Max messages per batch |
| `sink.flush-buffer.size` | Long | No | `5242880` | Max buffer size in bytes (5MB) |
| `sink.requests.max-buffered` | Int | No | `10000` | Max buffered requests |
| `sink.flush-buffer.timeout` | Long | No | `5000` | Flush timeout in ms |
| `sink.requests.max-inflight` | Int | No | `50` | Max concurrent HTTP requests |

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
- Increase `sink.requests.max-inflight` for high-throughput scenarios
- Use Enterprise API mode for higher throughput (no 20 msg/min limit)
- For webhook mode, batch messages at the Flink level before sending

---

---

# Flink 钉钉 Sink 连接器 - 使用手册

**版本:** 1.0.0-SNAPSHOT | **Flink:** 2.2.0 | **Java:** 11+

---

## 目录

1. [概述](#1-概述)
2. [架构设计](#2-架构设计)
3. [安装部署](#3-安装部署)
4. [获取凭证](#4-获取凭证)
5. [SQL API 使用](#5-sql-api-使用)
6. [DataStream API 使用](#6-datastream-api-使用)
7. [消息类型](#7-消息类型)
8. [配置参数参考](#8-配置参数参考)
9. [高级功能](#9-高级功能)
10. [测试](#10-测试)
11. [常见问题](#11-常见问题)

---

## 1. 概述

Flink 钉钉 Sink 连接器将 Flink 流数据作为钉钉消息发送。支持两种发送模式：

| 模式 | 使用场景 | 说明 |
|------|----------|------|
| **Webhook** | 群消息通知 | 通过群机器人 Webhook URL 发送到群聊 |
| **企业 API** | 单聊 / 定向发送 | 通过钉钉企业 API 发送给指定用户 |

### 支持的消息类型

| 类型 | 说明 | 必填字段 |
|------|------|----------|
| `text` | 纯文本消息 | `content` |
| `markdown` | 支持 Markdown 格式的富文本 | `title`, `content` |
| `actionCard` | 带操作按钮的卡片 | `title`, `content`，可选 `actionUrl`, `singleTitle` |
| `link` | 带链接的消息 | `title`, `content`, `messageUrl`，可选 `picUrl` |

---

## 2. 架构设计

```
+----------------+     +------------------+     +------------------+
|   Flink 数据源  |---->|  DingTalkSink    |---->|  钉钉服务器       |
|   (DataStream) |     |  (SinkV2 API)    |     |  (Webhook/API)   |
+----------------+     +------------------+     +------------------+
                              |
                              v
                       +------------------+
                       | 消息构建器        |
                       | (Jackson JSON)   |
                       +------------------+
                              |
                              v
                       +------------------+
                       | 钉钉客户端        |
                       | (Webhook/API)    |
                       +------------------+
```

连接器基于 Flink 的 **AsyncSinkBase** 实现异步批量发送，支持可配置的缓冲、批处理和重试机制。

---

## 3. 安装部署

### 3.1 从源码构建

```bash
git clone <repo-url>
cd dingding-flink-connector
mvn clean package -DskipTests
```

生成的 shaded JAR 位于 `target/dingtalk-flink-connector-1.0.0-SNAPSHOT.jar`。

### 3.2 部署到 Flink 集群

将 JAR 复制到 Flink 的 `lib/` 目录：

```bash
cp target/dingtalk-flink-connector-1.0.0-SNAPSHOT.jar $FLINK_HOME/lib/
```

或在提交任务时指定：

```bash
flink run -c com.example.YourJob \
  -C file:///path/to/dingtalk-flink-connector-1.0.0-SNAPSHOT.jar \
  your-job.jar
```

### 3.3 Maven 依赖

```xml
<dependency>
    <groupId>io.github.beryllw</groupId>
    <artifactId>dingtalk-flink-connector</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## 4. 获取凭证

### 4.1 Webhook 模式（群机器人）

1. 打开钉钉群聊 → **设置** → **智能群助手** → **添加机器人**
2. 选择 **自定义** 机器人
3. 安全设置选择 **加签**，复制 **Secret**
4. 复制 **Webhook 地址**

你将获得：
- `webhook`: `https://oapi.dingtalk.com/robot/send?access_token=xxx`
- `secret`: `SECxxx...`

### 4.2 企业 API 模式（单聊）

1. 打开 [钉钉开放平台](https://open-dev.dingtalk.com/)
2. 创建开启了 **机器人** 能力的企业内部应用
3. 在应用设置中记录：
   - **AppKey**（客户端 ID）
   - **AppSecret**（客户端密钥）
   - **RobotCode**（机器人标识，以 `ding` 开头）
4. 从钉钉管理后台或 API 获取目标用户的 **UserID**

你将获得：
- `app-key`: `dingxxxxxxxxx`
- `app-secret`: `xxxxxxxxxxxxxxxxxxxx`
- `robot-code`: `dingxxxxxxxxx`
- `user-ids`: 逗号分隔的用户 ID 列表

---

## 5. SQL API 使用

### 5.1 基础文本消息（Webhook）

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
SELECT '系统告警: CPU使用率超过90%' FROM alert_stream;
```

### 5.2 Markdown 消息带 @ 提醒

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
    '## 服务告警\n**服务名称:** order-service\n**错误率:** 15%\n**时间:** ' || CURRENT_TIMESTAMP,
    '服务告警'
FROM error_stream;
```

### 5.3 企业 API - 单聊发送

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

### 5.4 完整配置示例

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
    'retry-delay-ms' = '1000',
    'sink.batch.max-size' = '1',
    'sink.flush-buffer.timeout' = '5000',
    'sink.requests.max-inflight' = '50'
);
```

---

## 6. DataStream API 使用

### 6.1 基础用法

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

### 6.2 Builder 快捷方式

```java
stream.sinkTo(DingTalkSink.<String>builder()
        .setWebhook("https://oapi.dingtalk.com/robot/send?access_token=xxx")
        .setSecret("SECxxx")
        .setMessageType(MessageType.TEXT)
        .build());
```

### 6.3 企业 API 模式

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

### 6.4 自定义元素转换器

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

### 6.5 性能调优

```java
stream.sinkTo(DingTalkSink.<String>builder()
        .setOptions(options)
        .setMaxBatchSize(100)          // 每批最大消息数
        .setMaxTimeInBufferMS(1000)    // 每 1 秒刷新一次
        .setMaxInFlightRequests(50)    // 最大并发 HTTP 请求数
        .setMaxBufferedRequests(10000) // 最大缓冲消息数
        .build());
```

---

## 7. 消息类型

### 7.1 文本消息

```sql
-- SQL
INSERT INTO dingtalk_sink SELECT 'Hello World' AS content;
```

```java
// DataStream
options.setMessageType(MessageType.TEXT);
stream.sinkTo(DingTalkSink.<String>builder().setOptions(options).build());
```

### 7.2 Markdown 消息

```sql
-- SQL
INSERT INTO dingtalk_sink
SELECT
    '## 告警\n**CPU:** 95%\n**内存:** 80%' AS content,
    '系统告警' AS title;
```

```java
// DataStream
options.setMessageType(MessageType.MARKDOWN);
stream.map(e -> Map.of(
    "title", "系统告警",
    "content", "## 告警\n**CPU:** 95%"
)).sinkTo(DingTalkSink.<Map<String, String>>builder().setOptions(options).build());
```

### 7.3 卡片消息 (ActionCard)

```java
options.setMessageType(MessageType.ACTION_CARD);
stream.map(e -> Map.of(
    "title", "部署结果",
    "content", "## 部署成功\n应用: my-app\n版本: 1.2.0",
    "singleTitle", "查看详情",
    "actionUrl", "https://example.com/deploy/123"
)).sinkTo(DingTalkSink.<Map<String, String>>builder().setOptions(options).build());
```

### 7.4 链接消息 (Link)

```java
options.setMessageType(MessageType.LINK);
stream.map(e -> Map.of(
    "title", "新文章发布",
    "content", "查看最新的 Flink 性能优化文章。",
    "messageUrl", "https://example.com/blog/post-123",
    "picUrl", "https://example.com/image.png"
)).sinkTo(DingTalkSink.<Map<String, String>>builder().setOptions(options).build());
```

---

## 8. 配置参数参考

### SQL 表选项

| 选项 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `connector` | String | 是 | - | 必须为 `dingtalk` |
| `send-mode` | String | 否 | `webhook` | `webhook` 或 `api` |
| `webhook` | String | Webhook 模式必填 | - | Webhook URL（含 access_token） |
| `secret` | String | 否 | - | HMAC-SHA256 加签密钥 |
| `app-key` | String | API 模式必填 | - | 企业应用 AppKey |
| `app-secret` | String | API 模式必填 | - | 企业应用 AppSecret |
| `robot-code` | String | API 模式必填 | - | 机器人编码（以 `ding` 开头） |
| `user-ids` | String | API 模式必填 | - | 逗号分隔的用户 ID 列表 |
| `message-type` | String | 否 | `text` | `text`、`markdown`、`actionCard`、`link` |
| `at-mobiles` | String | 否 | - | 逗号分隔的手机号，用于 @ 指定用户 |
| `at-all` | Boolean | 否 | `false` | 是否 @ 所有人 |
| `max-retries` | Int | 否 | `3` | 失败时最大重试次数 |
| `retry-delay-ms` | Long | 否 | `1000` | 基础重试延迟（毫秒） |
| `sink.batch.max-size` | Int | 否 | `500` | 每批最大消息数 |
| `sink.flush-buffer.size` | Long | 否 | `5242880` | 最大缓冲区大小（字节，5MB） |
| `sink.requests.max-buffered` | Int | 否 | `10000` | 最大缓冲请求数 |
| `sink.flush-buffer.timeout` | Long | 否 | `5000` | 刷新超时时间（毫秒） |
| `sink.requests.max-inflight` | Int | 否 | `50` | 最大并发 HTTP 请求数 |

### 编程式配置 (DingTalkSinkOptions)

| 方法 | 类型 | 说明 |
|------|------|------|
| `setWebhook(String)` | String | 设置 Webhook URL |
| `setSecret(String)` | String | 设置加签密钥 |
| `setSendMode(SendMode)` | 枚举 | `WEBHOOK` 或 `API` |
| `setAppKey(String)` | String | 设置 API 模式的 AppKey |
| `setAppSecret(String)` | String | 设置 API 模式的 AppSecret |
| `setRobotCode(String)` | String | 设置 API 模式的机器人编码 |
| `setUserIds(List<String>)` | List | 设置目标用户 ID 列表 |
| `setMessageType(MessageType)` | 枚举 | `TEXT`、`MARKDOWN`、`ACTION_CARD`、`LINK` |
| `setAtMobiles(List<String>)` | List | 设置 @ 的手机号列表 |
| `setAtAll(boolean)` | boolean | 是否 @ 所有人 |
| `setMaxRetries(int)` | int | 最大重试次数 |
| `setRetryDelayMs(long)` | long | 基础重试延迟（毫秒） |
| `setConnectionTimeoutMs(int)` | int | HTTP 连接超时（毫秒），默认 `5000` |

---

## 9. 高级功能

### 9.1 限流机制（Webhook 模式）

钉钉 Webhook 限制为 **每分钟 20 条消息**。连接器使用令牌桶算法：
- 最大 20 个令牌
- 每 3 秒补充 1 个令牌
- 无可用令牌时请求等待

### 9.2 Token 缓存（API 模式）

企业 API 的 access_token 有效期为 7200 秒。连接器：
- 在内存中缓存 token
- 过期前 300 秒自动刷新
- 遇到 `40014`（无效 token）和 `42001`（token 过期）错误时自动重试

### 9.3 重试机制

失败的请求使用指数退避策略重试：

```
delay = retryDelayMs * 2^attempt
```

默认：1s, 2s, 4s（最多 3 次重试）。

可重试的错误码：
- `42001` - Token 过期
- `40014` - 无效的 Token
- `40001` - Access_token 不存在
- `45009` - 超出频率限制
- `42014` - 参数无效

### 9.4 字段映射

SQL 表的列名直接映射到消息字段：

| 列名 | 用于 | 用途 |
|------|------|------|
| `content` | 所有类型 | 消息正文 |
| `title` | markdown, actionCard, link | 消息标题 |
| `messageUrl` | link | 点击跳转链接 |
| `picUrl` | link | 缩略图 URL |
| `singleTitle` | actionCard | 按钮文字 |
| `actionUrl` | actionCard | 按钮跳转链接 |

---

## 10. 测试

### 10.1 单元测试

```bash
mvn test -Dtest=DingTalkMessageBuilderTest
```

### 10.2 DataStream 集成测试

```bash
mvn test -Dtest=DingTalkSinkDataStreamITCase
```

使用模拟 HTTP 服务器测试完整的 DataStream 链路。

### 10.3 SQL 集成测试

```bash
mvn test -Dtest=DingTalkSinkTableITCase
```

使用 MiniCluster 测试 SQL 表创建和 INSERT 操作。

### 10.4 真实 API 集成测试

```bash
# 设置凭证
source .env

# 运行测试
mvn test -Dtest=DingTalkApiClientIntegrationTest
```

---

## 11. 常见问题

### 常见错误

| 错误 | 原因 | 解决方案 |
|------|------|----------|
| `MissingrobotCode` | API 模式未设置 robotCode | 在表选项中设置 `robot-code` |
| `MissingmsgParam` | API 模式 payload 格式错误 | 确保正确设置 message-type |
| `robotCode not exist` | 无效的机器人编码 | 确认 robotCode 以 `ding` 开头 |
| `40014` / `42001` | Token 过期 | 连接器会自动刷新，检查 AppKey/AppSecret 是否正确 |
| `45009` | 超出频率限制 | 减小 batch size 或增加刷新间隔 |
| 连接超时 | 网络不可达 | 检查防火墙规则，验证 URL |

### 开启调试日志

在 `log4j.properties` 中启用调试日志：

```properties
log4j.logger.io.github.beryllw.dingtalk.connector=DEBUG
```

### 性能优化建议

- 设置 `sink.batch.max-size=1` 实现低延迟（即时发送）
- 高吞吐场景增加 `sink.requests.max-inflight`
- 使用企业 API 模式获得更高的发送频率（无 20 条/分钟限制）
- Webhook 模式下可在 Flink 层预聚合消息再发送
