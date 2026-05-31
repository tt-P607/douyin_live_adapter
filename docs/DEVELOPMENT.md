# Douyin Live Adapter 开发者指南

> 面向：插件维护者 / 协议升级者 / 调试者。
> 配套：[`README.md`](../README.md)（用户视角）、[`plans/douyin_live_adapter_design.md`](../../../plans/douyin_live_adapter_design.md)（设计原始稿）。

## 当前状态

- ✅ Phase 1-8 已完成（骨架、API 层、签名、Protobuf、WebSocket、分发、主类、文档）。
- ⏳ Phase 9 整合测试：需要真实抖音直播间环境长跑验证（未在本提交内执行）。
- 验证手段：``ruff check`` 与 ``ast.parse`` 全部通过；HTTP / signature / dispatcher
  三层均可单独 import 并通过最小烟雾测试。

---

## 1. 项目结构

```text
plugins/douyin_live_adapter/
├── manifest.json              # 插件声明（依赖、入口、版本）
├── README.md                  # 用户文档
├── LICENSE
├── config.py                  # ConfigBase / SectionBase 定义
├── plugin.py                  # 适配器主类 + 插件主类
├── docs/
│   └── DEVELOPMENT.md         # 本文件
├── assets/                    # 抖音网页版签名 JS（搬运自 DouyinLiveWebFetcher）
│   ├── sign.js
│   ├── a_bogus.js
│   └── webmssdk.js
└── src/
    ├── __init__.py
    ├── proto/                 # Protobuf 模型 + 解码工具
    │   ├── __init__.py        # 暴露业务类型 + decode_push_frame
    │   ├── douyin.proto       # 协议定义（搬运）
    │   └── douyin_pb2.py      # betterproto 生成代码（搬运）
    ├── signature.py           # JS 签名引擎（py-mini-racer）
    ├── api.py                 # HTTP 客户端（拿 ttwid / room_id）
    ├── client.py              # 异步 WebSocket 客户端（心跳 + recv loop）
    └── dispatcher.py          # method 路由 → MessageEnvelope
```

---

## 2. 数据流

```text
┌──────────────────────┐
│ DouyinLiveAdapter    │ ← 插件主类
│  ├─ session_loop     │ 拿 ttwid / room_id → client.start
│  ├─ reminder_loop    │ 周期性把 likes/viewers 写到 system_reminder
│  └─ _on_client_message
└──────────┬───────────┘
           │ method, payload
           ▼
┌──────────────────────┐    DispatchResult
│ DouyinDispatcher     │ ─────────────────► MessageEnvelope → core_sink.send
└──────────────────────┘                    或 signal=__live_ended__
           ▲
           │ method, payload
┌──────────────────────┐
│ DouyinClient         │
│  ├─ _heartbeat_loop  │  每 5s 发 hb 帧
│  └─ _recv_loop       │  解 PushFrame → gzip → Response → Message[]
│           │
│           └─ build_ack_frame() 自动回 ack
└──────────┬───────────┘
           │ wss URL = base + signature
           ▼
┌──────────────────────┐
│ DouyinSigner         │  py-mini-racer 跑 sign.js
└──────────────────────┘
```

---

## 3. 关键协议参考

### 3.1 长连握手时序

```text
1. GET https://live.douyin.com/                 → Cookie: ttwid
2. GET https://live.douyin.com/{live_id}        → 正则提 roomId
3. 拼 base WSS URL                              → 含 30+ 个 query
4. 提取 13 字段 → MD5 → JS get_sign(md5)        → 得 signature
5. 拼最终 WSS URL = base + &signature=xxx
6. 建立 ws，header 带 Cookie: ttwid + UA
7. 服务端推 PushFrame（gzip 压缩的 Response）
8. 客户端每 5s 发 PushFrame(payload_type='hb')
9. 若 Response.need_ack=true 立即回 PushFrame(payload_type='ack', ...)
```

### 3.2 PushFrame 与 Response 结构

```text
PushFrame {
  log_id: int64
  payload_type: string  // "msg" | "hb" | "ack"
  payload: bytes        // gzip 压缩的 Response
}

Response (gzip 解压后) {
  messages_list: repeated Message
  need_ack: bool
  internal_ext: string  // 用于 ack 回执
}

Message {
  method: string        // "WebcastChatMessage" 等
  payload: bytes        // 具体消息体
}
```

详见 [`src/proto/__init__.py`](../src/proto/__init__.py) 的 ``decode_push_frame``。

### 3.3 业务消息映射

| method | 含义 | 是否生成 envelope | 备注 |
|--------|------|-----------------|------|
| ``WebcastChatMessage`` | 弹幕 | ✅ | 文本消息 |
| ``WebcastGiftMessage`` | 礼物 | ✅ | 文本前缀 ``[送出礼物]`` |
| ``WebcastSocialMessage`` | 关注 | ✅（仅 action=1） | 文本 ``[关注主播]`` |
| ``WebcastLikeMessage`` | 点赞 | ❌ | 累加 ``total_likes`` |
| ``WebcastRoomUserSeqMessage`` | 在线统计 | ❌ | 更新 ``current_viewers`` |
| ``WebcastMemberMessage`` | 进场 | 默认❌（可开） | 噪音大 |
| ``WebcastControlMessage`` | 直播状态 | ❌ | status=3 触发停止 |

---

## 4. 调试技巧

### 4.1 单独测试签名

```python
from pathlib import Path
from plugins.douyin_live_adapter.src.signature import DouyinSigner

signer = DouyinSigner(assets_dir=Path("plugins/douyin_live_adapter/assets"))
url = "wss://...&room_id=123&user_unique_id=456&aid=6383..."  # 真实抓到的
print(signer.gen_signature(url))
```

### 4.2 单独测试 HTTP 层

```python
import asyncio
from plugins.douyin_live_adapter.src.api import DouyinApi

async def main():
    api = DouyinApi(user_agent="Mozilla/5.0 ...")
    ttwid = await api.get_ttwid()
    room_id = await api.get_room_id("123456789", ttwid=ttwid)
    print(ttwid, room_id)
    await api.aclose()

asyncio.run(main())
```

### 4.3 关闭重连观察一次错误

把 ``config.connection.auto_reconnect`` 改成 ``false``；插件遇到任何异常会
直接退出会话循环，方便定位根因。

---

## 5. 协议升级流程

抖音 Web 协议偶尔会变更，常见变更点：

1. **签名算法 (sign.js) 升级**：从 [`DouyinLiveWebFetcher`][repo] 仓库拿最新
   ``sign.js`` 直接覆盖 ``assets/sign.js``，无需改 Python 代码。
2. **roomId 提取失效**：检查抖音前端 HTML 是否改了字段名；编辑
   [`src/api.py`](../src/api.py) 中的 ``_ROOM_ID_PATTERNS``。
3. **WSS host 变更**：将新的 host 写入 [`src/client.py`](../src/client.py) 的
   ``_DEFAULT_WSS_HOST`` 常量。
4. **Protobuf 字段变更**：从上游同步 ``protobuf/douyin.proto`` 与
   ``protobuf/douyin.py`` 到 ``src/proto/`` 即可。

[repo]: https://github.com/saermart/DouyinLiveWebFetcher

---

## 6. 已知问题与限制

- **不支持出站**：抖音 Web 协议不允许第三方 Bot 发弹幕，
  ``_send_platform_message`` 是 no-op。
- **不能跨账号识别同一用户**：抖音的 ``id_str`` 仅在该直播间内稳定；跨直播
  间需要使用 ``sec_uid`` 但格式不同。
- **Member 进场事件默认关闭**：抖音直播间进场频率非常高，开启会把 chatter
  拉满垃圾消息；如需开启请改 dispatcher 构造参数 ``emit_member_events=True``。
- **总观看人数 (``total_pv_for_anchor``) 抖音可能返回 ``"1.2万"`` 这种字符串**：
  当前实现仅在能 ``int()`` 时记录，无法解析中文后缀。

---

## 7. 与 B 站插件的差异

| 维度 | B 站插件 | 抖音插件 |
|------|---------|---------|
| 协议来源 | 开放平台（需要凭证） | Web 模拟（无凭证） |
| 出站能力 | 不允许 | 不允许 |
| 心跳协议 | op=2（自定义二进制）+ HTTP 心跳 | PushFrame(payload_type='hb') |
| 签名 | access_key 走 HMAC-SHA256 | sign.js 走 V8 |
| 房间生命 | game_id（30 分钟锁） | room_id（永久） |
| 下播感知 | HTTP 接口轮询 | WebcastControlMessage(status=3) |

---

## 8. 测试建议（后续待补）

- ``test/plugins/douyin_live_adapter/test_signature.py``：mock JS 引擎，验证
  签名调用入参。
- ``test/plugins/douyin_live_adapter/test_dispatcher.py``：feed 标准 Protobuf
  样本，断言 envelope 结构。
- ``test/plugins/douyin_live_adapter/test_proto.py``：用样本字节验证
  ``decode_push_frame`` 完整链路。
