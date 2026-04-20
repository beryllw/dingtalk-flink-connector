# Flink DingTalk Sink Connector

[English](README.md) | [中文](README_zh.md)

---

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
  - [1. Add Dependency](#1-add-dependency)
  - [2. SQL Usage](#2-sql-usage)
  - [3. DataStream API Usage](#3-datastream-api-usage)
- [Configuration Options](#configuration-options)
- [Getting Credentials](#getting-credentials)
  - [Webhook Mode](#webhook-mode)
  - [Enterprise API Mode](#enterprise-api-mode)
- [Running Tests](#running-tests)
- [User Guide](#user-guide)

---

## Features

- **SinkV2 API** - Compatible with Flink 2.2.0+
- **Dual Mode** - Webhook or Enterprise API
- **SQL Support** - `CREATE TABLE ... WITH ('connector' = 'dingtalk')`
- **Message Types** - text, markdown, actionCard, link
- **Rate Limiting** - Built-in token bucket for Webhook (20 msg/min)
- **Token Caching** - Auto-refresh access_token (7200s TTL)
- **Retry Logic** - Exponential backoff with configurable retries

## Quick Start

### 1. Add Dependency

Build the JAR:
```bash
mvn clean package -DskipTests
```

Copy the shaded JAR to Flink's `lib/` directory.

### 2. SQL Usage

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

### 3. DataStream API Usage

```java
DingTalkSink<String> sink = DingTalkSink.<String>builder()
        .setWebhook("https://oapi.dingtalk.com/robot/send?access_token=xxx")
        .setMessageType(MessageType.TEXT)
        .build();

stream.sinkTo(sink);
```

## Configuration Options

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

## Getting Credentials

### Webhook Mode
1. Open DingTalk group chat → Settings → Intelligent Assistant → Add Robot
2. Copy the Webhook URL and signing secret

### Enterprise API Mode
1. Go to [DingTalk Open Platform](https://open-dev.dingtalk.com/)
2. Create an enterprise app with robot capability
3. Get AppKey, AppSecret, and RobotCode from app settings

## Running Tests

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

## User Guide

For detailed usage instructions, see the [User Guide](USER_GUIDE.md).
