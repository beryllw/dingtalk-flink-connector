# Flink DingTalk Sink Connector

[English](#english) | [中文](#中文)

---

## English

A Flink SQL / DataStream Sink Connector that sends stream data as DingTalk messages. Supports both Webhook (group robot) and Enterprise API (single chat) modes.

### Features

- **SinkV2 API** - Compatible with Flink 1.18+
- **Dual Mode** - Webhook or Enterprise API
- **SQL Support** - `CREATE TABLE ... WITH ('connector' = 'dingtalk')`
- **Message Types** - text, markdown, actionCard, link
- **Rate Limiting** - Built-in token bucket for Webhook (20 msg/min)
- **Token Caching** - Auto-refresh access_token (7200s TTL)
- **Retry Logic** - Exponential backoff with configurable retries

### Quick Start

#### 1. Add Dependency

Build the JAR:
```bash
mvn clean package -DskipTests
```

Copy the shaded JAR to Flink's `lib/` directory.

#### 2. SQL Usage

**Webhook Mode:**
```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'webhook',
    'webhook' = 'https://oapi.dingtalk.com/robot/send?access_token=xxx',
    'secret' = 'your-signing-secret',
    'message-type' = 'markdown'
);

INSERT INTO dingtalk_sink
SELECT 'Alert: CPU usage exceeds 90%', 'System Alert' FROM alert_stream;
```

**Enterprise API Mode:**
```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'api',
    'app-key' = 'your-app-key',
    'app-secret' = 'your-app-secret',
    'robot-code' = 'your-robot-code',
    'user-ids' = 'user001,user002',
    'message-type' = 'markdown'
);
```

#### 3. DataStream API Usage

```java
DingTalkSink<String> sink = DingTalkSink.<String>builder()
        .setWebhook("https://oapi.dingtalk.com/robot/send?access_token=xxx")
        .setMessageType(MessageType.TEXT)
        .build();

stream.addSink(sink);
```

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `connector` | String | Yes | Must be `dingtalk` |
| `send-mode` | String | No | `webhook` or `api`, default `webhook` |
| `webhook` | String | Yes (Webhook) | Webhook URL |
| `secret` | String | No | Webhook signing secret |
| `app-key` | String | Yes (API) | Enterprise app AppKey |
| `app-secret` | String | Yes (API) | Enterprise app AppSecret |
| `robot-code` | String | Yes (API) | Robot code from app settings |
| `user-ids` | String | Yes (API) | Comma-separated user IDs |
| `message-type` | String | No | `text`, `markdown`, `actionCard`, `link`, default `text` |
| `at-mobiles` | String | No | Comma-separated mobile numbers to @ |
| `at-all` | Boolean | No | @ everyone, default `false` |
| `max-retries` | Int | No | Max retries, default `3` |
| `retry-delay-ms` | Long | No | Retry delay in ms, default `1000` |
| `sink.batch.max-size` | Int | No | Max batch size, default `500` |
| `sink.flush-buffer.timeout` | Long | No | Flush timeout in ms, default `5000` |

### Getting Credentials

#### Webhook Mode
1. Open DingTalk group chat → Settings → Intelligent Assistant → Add Robot
2. Copy the Webhook URL and signing secret

#### Enterprise API Mode
1. Go to [DingTalk Open Platform](https://open-dev.dingtalk.com/)
2. Create an enterprise app with robot capability
3. Get AppKey, AppSecret, and RobotCode from app settings

### Running Tests

```bash
# Unit tests
mvn test -Dtest=DingTalkMessageBuilderTest

# Integration tests (set env vars first)
export DINGTALK_APP_KEY=your-app-key
export DINGTALK_APP_SECRET=your-app-secret
export DINGTALK_ROBOT_CODE=your-robot-code
export DINGTALK_USER_ID=your-user-id

mvn test -Dtest=DingTalkApiClientIntegrationTest
```

---

## 中文

Flink SQL / DataStream Sink 连接器，将流数据作为钉钉消息发送。支持 Webhook（群机器人）和企业 API（单聊）两种模式。

### 特性

- **SinkV2 API** - 兼容 Flink 1.18+
- **双模式** - Webhook 或企业 API
- **SQL 支持** - `CREATE TABLE ... WITH ('connector' = 'dingtalk')`
- **消息类型** - text、markdown、actionCard、link
- **限流机制** - Webhook 内置令牌桶（20 条/分钟）
- **Token 缓存** - access_token 自动刷新（7200 秒过期）
- **重试机制** - 指数退避，可配置重试次数

### 快速开始

#### 1. 构建 JAR

```bash
mvn clean package -DskipTests
```

将生成的 shaded JAR 复制到 Flink 的 `lib/` 目录。

#### 2. SQL 使用

**Webhook 模式：**
```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'webhook',
    'webhook' = 'https://oapi.dingtalk.com/robot/send?access_token=xxx',
    'secret' = '你的加签密钥',
    'message-type' = 'markdown'
);

INSERT INTO dingtalk_sink
SELECT '告警: CPU使用率超过90%', '系统告警' FROM alert_stream;
```

**企业 API 模式：**
```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'api',
    'app-key' = '你的AppKey',
    'app-secret' = '你的AppSecret',
    'robot-code' = '你的RobotCode',
    'user-ids' = 'user001,user002',
    'message-type' = 'markdown'
);
```

#### 3. DataStream API 使用

```java
DingTalkSink<String> sink = DingTalkSink.<String>builder()
        .setWebhook("https://oapi.dingtalk.com/robot/send?access_token=xxx")
        .setMessageType(MessageType.TEXT)
        .build();

stream.addSink(sink);
```

### 配置选项

| 选项 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `connector` | String | 是 | 必须为 `dingtalk` |
| `send-mode` | String | 否 | `webhook` 或 `api`，默认 `webhook` |
| `webhook` | String | Webhook 必填 | Webhook URL |
| `secret` | String | 否 | Webhook 加签密钥 |
| `app-key` | String | API 必填 | 企业应用 AppKey |
| `app-secret` | String | API 必填 | 企业应用 AppSecret |
| `robot-code` | String | API 必填 | 应用的机器人编码 |
| `user-ids` | String | API 必填 | 逗号分隔的用户 ID 列表 |
| `message-type` | String | 否 | `text`、`markdown`、`actionCard`、`link`，默认 `text` |
| `at-mobiles` | String | 否 | 逗号分隔的手机号，用于 @ 指定用户 |
| `at-all` | Boolean | 否 | 是否 @ 所有人，默认 `false` |
| `max-retries` | Int | 否 | 最大重试次数，默认 `3` |
| `retry-delay-ms` | Long | 否 | 重试间隔（毫秒），默认 `1000` |
| `sink.batch.max-size` | Int | 否 | 批量大小，默认 `500` |
| `sink.flush-buffer.timeout` | Long | 否 | 刷新超时（毫秒），默认 `5000` |

### 获取凭证

#### Webhook 模式
1. 打开钉钉群聊 → 设置 → 智能群助手 → 添加机器人
2. 复制 Webhook URL 和加签密钥

#### 企业 API 模式
1. 打开 [钉钉开放平台](https://open-dev.dingtalk.com/)
2. 创建带有机器人能力的企业应用
3. 在应用设置中获取 AppKey、AppSecret 和 RobotCode

### 运行测试

```bash
# 单元测试
mvn test -Dtest=DingTalkMessageBuilderTest

# 集成测试（需先设置环境变量）
export DINGTALK_APP_KEY=your-app-key
export DINGTALK_APP_SECRET=your-app-secret
export DINGTALK_ROBOT_CODE=your-robot-code
export DINGTALK_USER_ID=your-user-id

mvn test -Dtest=DingTalkApiClientIntegrationTest
```
