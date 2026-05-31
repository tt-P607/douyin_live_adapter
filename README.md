# 抖音直播弹幕适配器（Douyin Live Adapter）

基于抖音 Web 协议（模拟浏览器访问 `live.douyin.com`）实现的直播弹幕入站适配器。

> 本插件**只入站**：抖音 Web 协议不允许第三方 Bot 主动发送弹幕。Bot 的回应需要由
> 其它出站插件（如 [`anima_chatter`](../anima_chatter/) + VTube Studio + TTS）完成。

## 功能特性

- 异步长连接（基于 `websockets` + `httpx`）。
- 自动获取 `ttwid` 与真实 `room_id`（``live_id`` 是主播账号的 web_rid，长期有效）。
- 通过 `py-mini-racer` 执行抖音网页版 JS 签名算法（``signature``）。
- 支持消息类型：
  - 弹幕（`WebcastChatMessage`）
  - 礼物（`WebcastGiftMessage`）
  - 关注主播（`WebcastSocialMessage`，仅 `action=1`）
  - 直播状态（`WebcastControlMessage`，`status=3` 时主动停止重连）
  - 点赞（`WebcastLikeMessage`，累加并写入 `system_reminder`）
  - 在线人数（`WebcastRoomUserSeqMessage`，累加并写入 `system_reminder`）
- 指数退避自动重连。
- 主播下播时优雅退出，不疯狂重连。

## 与多平台直播协同

本插件与 [`bilibili_live_adapter`](../bilibili_live_adapter/) 共享一套"合并 stream"约定，
让 B 站 + 抖音同时直播时能进入**同一个聊天会话**，由 anima_chatter 串行决策、避免两边
chatter / VTS 打架：

- envelope 的 `platform` 统一为 `"live"`，`group_id` 统一为 `"live_room"`，因此两个平台的
  弹幕计算出来的 `stream_id` 完全相同。
- 真实来源由 `additional_config.source_platform`（也透传到 `Message.extra`）携带，
  本插件这里永远是 `"douyin_live"`。
- 真实 `room_id` 由 `additional_config.source_room_id` 携带。

## 快速上手

### 1. 拿到 `live_id`

打开主播的 PC 端直播间页面，URL 形如：

```
https://live.douyin.com/<live_id>
```

末段那串数字（即 `<live_id>`，也叫 web_rid）就是要填的值。**对每个抖音账号是长期固定的**——
同一个主播每次开播都是这个值，不需要更新；只有底层 `room_id` 每次开播变化，但插件会自动
重新获取，无需用户介入。

> 注意：分享短链（`v.douyin.com/xxxxxx`）会跳转到移动端 reflow URL，**不能直接当 `live_id`** 用。

### 2. 配置插件

在 `config/plugins/douyin_live_adapter/config.toml` 中填：

```toml
[plugin]
# 是否启用本插件的长连功能；关闭后不会建立任何抖音 WS 长连
enabled = true

[douyin]
# 直播间 ID（即 https://live.douyin.com/{live_id} 中的 live_id 部分）
live_id = ""

# 聊天流（直播间）的显示名；留空则用 "抖音直播间 {room_id}" 兜底。
# 多平台同播时建议两个 adapter 填同一个值，stream 才能用统一名字。
stream_name = ""

# 模拟浏览器的 User-Agent（一般不用改）
user_agent = "Mozilla/5.0 ..."

# 可选：手动 Cookie；留空即可由插件自动获取 ttwid
cookie = ""

[connection]
heartbeat_interval = 5.0       # WebSocket 心跳间隔（秒）
auto_reconnect = true          # 长连断开后是否自动重连
reconnect_initial_delay = 2.0  # 首次重连延迟
reconnect_max_delay = 60.0     # 重连退避封顶
request_timeout = 10.0         # HTTP 超时

[signature]
retry_max = 3                  # 签名偶发返回空时的重试次数
```

### 3. 启动 Bot

```bash
uv run main.py
```

预期日志关键字（按顺序）：

| 关键字 | 含义 |
|--------|------|
| `成功获取 ttwid` | HTTP 首页拿 cookie 成功 |
| `提取 roomId 成功: live_id=xxx -> room_id=yyy` | 拿到本次开播的真实 `room_id` |
| `连接抖音长连：room_id=...` | 即将建立 WebSocket |
| `抖音 WebSocket 连接成功` | 长连握手 + 鉴权完成 |
| `收到弹幕 [room=...] 用户名(sec_uid): 内容` | 真实业务消息流入 |

## 平台标识

| 字段 | 值 |
|------|------|
| envelope `platform` | `"live"`（合并 stream 用的虚拟平台名） |
| envelope `group_id` | `"live_room"`（合并 stream 用的虚拟群 ID） |
| envelope `group_name` | 配置 `stream_name` 优先；留空时 `"抖音直播间 {room_id}"` |
| `source_platform` | `"douyin_live"`（注入到 `additional_config` 与 `message_info.extra`） |
| 用户标识 | `User.sec_uid`（跨直播间稳定，相当于 B 站的 `open_id`） |

## 系统依赖

`manifest.json` 已声明，框架会自动安装：

- `websockets >= 12.0`
- `httpx >= 0.27.0`
- `py-mini-racer >= 0.6.0`
- `betterproto >= 1.2.5`

> `py-mini-racer` 在 Python 3.11 主流平台（Windows/Linux/macOS x86_64）有预编译 wheel，
> **无需 VS Build Tools / Node.js**。当前实测可用版本为 `0.6.0`，与上游
> [`DouyinLiveWebFetcher`][repo] 一致。

## 风险与协议升级

抖音 Web 协议不是公开 API，存在协议变更风险：

- **`signature` 算法换版**：从 [`DouyinLiveWebFetcher`][repo] 拿最新 `sign.js` 直接覆盖
  `assets/sign.js`，无需改 Python 代码。
- **`roomId` 提取失效**：抖音前端 HTML 结构变了。检查 `src/api.py` 中的正则
  `_ROOM_ID_PATTERNS`。
- **WSS host 变更**：调整 `src/client.py` 中的 `_DEFAULT_WSS_HOST`。

[repo]: https://github.com/saermart/DouyinLiveWebFetcher

请确保使用合规，仅用于学习与个人 VTB 联动用途。

## 协同插件

- 与 [`anima_chatter`](../anima_chatter/) 联动可实现"弹幕进 → TTS 出 → VTube 形象动起来"。
- 与 [`bilibili_live_adapter`](../bilibili_live_adapter/) 完全独立，可同时启用并自动合并 stream。
- 任何消费 `MessageEnvelope` 的下游插件都能按 `additional_config.source_platform` 区分本插件来源。

## 文档

- [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) — 开发者指南、调试技巧、协议参考。

## 许可

[AGPL-v3.0](LICENSE)
