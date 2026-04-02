# Flink DingTalk Sink Connector 方案

## Context

需要实现一个 Flink SQL / DataStream Sink Connector，将 Flink 流数据转换为钉钉消息（群机器人Webhook或企业内部应用API）发送给用户。目标 Flink 版本为 1.20 或 2.2，需同时支持 DataStream API 和 SQL API。

## 钉钉消息发送方式

### 方式一：群机器人 Webhook（简单场景）
- **Endpoint**: `POST https://oapi.dingtalk.com/robot/send?access_token=xxx`
- **认证**: Webhook URL + 签名（HMAC-SHA256 或加签）
- **限制**: 每分钟最多发送 20 条消息，仅支持群聊
- **消息格式**: text, markdown, link, actionCard

### 方式二：企业内部应用机器人 API（完整场景）
- **Endpoint**: `POST https://oapi.dingtalk.com/v1.0/robot/oToMessages/batchSend`
- **认证**: AppKey + AppSecret -> 获取 access_token
- **功能**: 支持单聊消息、群聊消息、工作通知
- **消息格式**: 更丰富，支持 text, markdown, actionCard, sampleCard, interactive card 等

## 整体架构

```
+-------------------+     +-------------------+     +-------------------+
|   Flink Source    |---->| DingTalk Sink     |---->| DingTalk API      |
|   (DataStream)    |     |   Writer          |     |   (Webhook/HTTP)  |
+-------------------+     +-------------------+     +-------------------+
                                  |
                                  v
                          +-------------------+
                          |   DingTalk Sink   |
                          |   Table Factory   |
                          |   (SQL Support)   |
                          +-------------------+
```

## 模块结构

```
dingtalk-flink-connector/
├── pom.xml
└── src/main/java/com/example/flink/connector/dingtalk/
    ├── sink/
    │   ├── DingTalkSink.java              # DataStream Sink 入口
    │   ├── DingTalkSinkBuilder.java        # Sink 构建器
    │   ├── DingTalkSinkWriter.java         # SinkV2 Writer 实现
    │   ├── DingTalkSinkFunction.java       # RichSinkFunction (DataStream 兼容)
    │   └── DingTalkCommitter.java          # SinkV2 Committer (可选)
    ├── table/
    │   ├── DingTalkDynamicTableFactory.java # SQL Table Factory
    │   └── DingTalkOptions.java            # SQL 配置选项
    ├── client/
    │   ├── DingTalkClient.java             # HTTP 客户端接口
    │   ├── DingTalkWebhookClient.java      # Webhook 实现
    │   ├── DingTalkApiClient.java          # 企业内部应用 API 实现
    │   └── DingTalkMessageBuilder.java     # 消息构建器
    └── config/
        ├── DingTalkSinkOptions.java        # 配置类
        ├── SendMode.java                   # 枚举: WEBHOOK / API
        └── MessageType.java                # 枚举: TEXT / MARKDOWN / ACTIONCARD / LINK
```

## 核心设计

### 1. 配置选项 (DingTalkSinkOptions)

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `webhook` | String | 是(二选一) | Webhook URL |
| `secret` | String | 否 | Webhook 加签密钥 |
| `app-key` | String | 是(二选一) | 企业内部应用 AppKey |
| `app-secret` | String | 是(二选一) | 企业内部应用 AppSecret |
| `send-mode` | Enum | 是 | `webhook` 或 `api` |
| `message-type` | String | 否 | `text`/`markdown`/`actionCard`/`link`，默认 `text` |
| `at-mobiles` | String | 否 | @指定手机号，逗号分隔 |
| `at-all` | Boolean | 否 | 是否@所有人，默认 false |
| `user-id-field` | String | 否 | API模式下，指定userid字段名 |
| `max-retries` | Int | 否 | 最大重试次数，默认 3 |
| `retry-delay-ms` | Long | 否 | 重试间隔(ms)，默认 1000 |
| `batch-size` | Int | 否 | 批量发送大小，默认 1 |
| `connection-timeout-ms` | Int | 否 | 连接超时，默认 5000 |

### 2. SinkV2 Writer 实现

使用 Flink SinkV2 SPI (`Sink.Writer` 接口)：

- **write** 方法：将 InputRecord 序列化为 HTTP 请求，调用钉钉 API
- **snapshotState**：保存发送状态（用于 checkpoint）
- 使用 `HttpURLConnection` 或 `OkHttp` 作为底层 HTTP 客户端
- 内部维护 access_token 缓存（API 模式，token 有效期 7200 秒）

### 3. SQL Table Factory

实现 `DynamicTableSinkFactory` 接口：

- **factoryIdentifier**: `dingtalk`
- **requiredOptions**: `webhook` 或 `app-key` + `app-secret`
- **optionalOptions**: 其余配置项
- SQL 使用示例：

```sql
CREATE TABLE dingtalk_sink (
    content STRING,
    title STRING,
    userid STRING
) WITH (
    'connector' = 'dingtalk',
    'send-mode' = 'api',
    'app-key' = 'your-app-key',
    'app-secret' = 'your-app-secret',
    'message-type' = 'markdown',
    'user-id-field' = 'userid'
);

INSERT INTO dingtalk_sink
SELECT '告警: CPU使用率超过90%', '系统告警', 'user001' FROM alert_stream;
```

### 4. 消息构建

根据 `message-type` 和输入字段动态构建消息体：

- **text**: `{"msgtype":"text","text":{"content":"..."}}`
- **markdown**: `{"msgtype":"markdown","markdown":{"title":"...","text":"..."}}`
- **actionCard**: `{"msgtype":"actionCard","actionCard":{"title":"...","text":"...","btnOrientation":"0","btns":[...]}}`
- **link**: `{"msgtype":"link","link":{"title":"...","text":"...","messageUrl":"...","picUrl":"..."}}`

输入字段映射：通过字段名或位置映射到消息模板参数。

## 关键实现决策

| 决策 | 选择 | 理由 |
|------|------|------|
| Sink API | SinkV2 (Flink 1.20/2.2) | 新版本标准，支持两阶段提交 |
| HTTP 客户端 | HttpURLConnection (JDK内置) | 零外部依赖，轻量 |
| Token 缓存 | 内存 + TTL 刷新 | 简单有效，7200s 过期前刷新 |
| 序列化 | Jackson (JSON) | 钉钉 API 使用 JSON，Flink 生态常用 |
| 打包方式 | Fat JAR (Maven Shade) | 零外部依赖，便于部署 |
| 限流策略 | 内置令牌桶限流 | Webhook 模式每分钟20条限制 |
| 日志 | SLF4J | 标准做法 |

## 风险与注意事项

1. **限流**: Webhook 模式每分钟最多 20 条，需考虑限流策略或切换到 API 模式
2. **Checkpoint**: Writer 需实现 `snapshotState` 保证 exactly-once 语义
3. **异常处理**: 网络异常需重试，钉钉 API 返回错误码需特殊处理（如 token 过期）
4. **依赖管理**: 尽量使用 shading 打包，避免与用户集群的依赖冲突

## 验证方案

1. **单元测试**: Mock HTTP 响应，测试消息构建和发送逻辑
2. **集成测试**: 使用 Flink MiniCluster 测试 Sink 完整流程
3. **SQL 测试**: 通过 SQL Client 验证 CREATE TABLE 和 INSERT
4. **手动测试**: 连接真实钉钉环境验证消息发送
