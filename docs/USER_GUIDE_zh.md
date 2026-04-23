# Flink 钉钉 Sink 连接器 - 使用手册

**版本:** 1.0.0-SNAPSHOT | **Flink:** 1.20 / 2.2 | **Java:** 11+

[English](USER_GUIDE.md) | [中文](USER_GUIDE_zh.md)

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
|   Flink 数据源  |---->|  DingTalkSink    |---->|  钉钉服务器        |
|   (DataStream) |     |  (SinkV2 API)    |     |  (Webhook/API)   |
+----------------+     +------------------+     +------------------+
                              |
                              v
                       +------------------+
                       | 消息构建器         |
                       | (Jackson JSON)   |
                       +------------------+
                              |
                              v
                       +------------------+
                       | 钉钉客户端         |
                       | (Webhook/API)    |
                       +------------------+
```

连接器基于 Flink 的 **AsyncSinkBase** 实现异步批量发送，支持可配置的缓冲、批处理和重试机制。

---

## 3. 安装部署

### 3.1 从 GitHub Releases 下载

前往 [GitHub Releases](https://github.com/beryllw/dingtalk-flink-connector/releases) 下载对应 Flink 版本的 shaded JAR：

| 文件 | Flink 版本 |
|------|------------|
| `dingtalk-flink-connector-1.20-<version>.jar` | Flink 1.20.x |
| `dingtalk-flink-connector-2.2-<version>.jar` | Flink 2.2.x |

### 3.2 从源码构建

```bash
git clone https://github.com/beryllw/dingtalk-flink-connector.git
cd dingtalk-flink-connector
mvn clean package -DskipTests
```

生成的 shaded JAR 位于：
- `dingtalk-flink-connector-1.20/target/dingtalk-flink-connector-1.20-<version>.jar`（适用于 Flink 1.20）
- `dingtalk-flink-connector-2.2/target/dingtalk-flink-connector-2.2-<version>.jar`（适用于 Flink 2.2）

### 3.3 部署到 Flink 集群

将 JAR 复制到 Flink 的 `lib/` 目录：

```bash
# Flink 1.20
cp dingtalk-flink-connector-1.20-<version>.jar $FLINK_HOME/lib/

# Flink 2.2
cp dingtalk-flink-connector-2.2-<version>.jar $FLINK_HOME/lib/
```

或在提交任务时指定：

```bash
flink run -c com.example.YourJob \
  -C file:///path/to/dingtalk-flink-connector-1.20-<version>.jar \
  your-job.jar
```

### 3.4 Maven 依赖（本地安装）

如果需要在自己的项目中以 Maven 依赖方式引入连接器（例如使用 DataStream API 开发），需先将连接器安装到本地 Maven 仓库：

```bash
git clone https://github.com/beryllw/dingtalk-flink-connector.git
cd dingtalk-flink-connector
mvn clean install -DskipTests
```

然后在项目的 `pom.xml` 中添加依赖：

```xml
<!-- Flink 1.20 -->
<dependency>
    <groupId>io.github.beryllw</groupId>
    <artifactId>dingtalk-flink-connector-1.20</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>

<!-- Flink 2.2 -->
<dependency>
    <groupId>io.github.beryllw</groupId>
    <artifactId>dingtalk-flink-connector-2.2</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

> **注意：** 本连接器尚未发布到 Maven Central，使用上述坐标前必须先执行本地安装。如果仅使用 Flink SQL 方式，无需本地安装，只需将 shaded JAR 部署到 Flink 集群即可（参见 3.3 节）。

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
    'retry-delay-ms' = '1000'
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
| `user-id-field` | String | 否 | - | 指定包含动态用户 ID 的列名（API 模式） |
| `message-type` | String | 否 | `text` | `text`、`markdown`、`actionCard`、`link` |
| `at-mobiles` | String | 否 | - | 逗号分隔的手机号，用于 @ 指定用户 |
| `at-all` | Boolean | 否 | `false` | 是否 @ 所有人 |
| `max-retries` | Int | 否 | `3` | 失败时最大重试次数 |
| `retry-delay-ms` | Long | 否 | `1000` | 基础重试延迟（毫秒） |
| `sink.batch.max-size` | Int | 否 | `20` | 每批最大记录数（适配钉钉 ~20 条/分钟限流） |
| `sink.max-in-flight-requests` | Int | 否 | `1` | 最大并发异步请求数 |
| `sink.max-buffered-requests` | Int | 否 | `100` | 缓冲区最大请求数（达到后触发反压） |
| `sink.buffer.flush-interval` | Long | 否 | `5000` | 缓冲区刷新间隔（毫秒） |
| `sink.buffer.max-size-in-bytes` | Long | 否 | `5242880` | 缓冲区最大字节数（5 MB） |

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
| `setUserIdField(String)` | String | 指定包含动态用户 ID 的列名（API 模式） |
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
- 高吞吐场景增加 `sink.max-in-flight-requests`
- 使用企业 API 模式获得更高的发送频率（无 20 条/分钟限制）
- Webhook 模式下可在 Flink 层预聚合消息再发送
