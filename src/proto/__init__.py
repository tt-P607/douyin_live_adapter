"""douyin_live_adapter Protobuf 模型与解码工具。

本包封装了抖音 Web 协议的 Protobuf 解码能力，对外只暴露：

- 业务消息类型（``ChatMessage`` / ``GiftMessage`` / ``LikeMessage`` / ...）
- ``PushFrame`` 与 ``Response`` 容器类型
- :func:`decode_push_frame` 一站式解码函数

底层 Protobuf 类型由 ``douyin_pb2`` 模块提供（基于 ``betterproto`` 生成的代码，
原始来源：https://github.com/saermart/DouyinLiveWebFetcher/blob/main/protobuf/douyin.py）。
"""

from __future__ import annotations

import gzip
from dataclasses import dataclass

from .douyin_pb2 import (
    ChatMessage,
    Common,
    ControlMessage,
    EmojiChatMessage,
    FansclubMessage,
    GiftMessage,
    Image,
    LikeMessage,
    MemberMessage,
    Message,
    PushFrame,
    Response,
    RoomMessage,
    RoomRankMessage,
    RoomStatsMessage,
    RoomStreamAdaptationMessage,
    RoomUserSeqMessage,
    SocialMessage,
    User,
)


@dataclass(frozen=True)
class DecodedFrame:
    """解码后的一帧 ``PushFrame`` 内容。

    Attributes:
        log_id: 该帧在抖音侧的 log_id，回 ack 时必须原样带回。
        need_ack: 抖音是否要求客户端立即回 ack 帧。
        internal_ext: 抖音侧返回的扩展信息，用作 ack 的 payload。
        messages: 该帧内含的业务消息（已按 method 分组，未解析具体 payload）。
    """

    log_id: int
    need_ack: bool
    internal_ext: str
    messages: list[Message]


def decode_push_frame(raw: bytes) -> DecodedFrame:
    """对一帧 ``PushFrame`` 二进制做完整解码。

    Args:
        raw: 抖音 WebSocket 侧推过来的二进制帧。

    Returns:
        解码后的 :class:`DecodedFrame`，已经把 gzip ``payload`` 解压并解析为 ``Response``。

    Raises:
        ValueError: 帧格式异常或 gzip 解压失败时抛出。
    """
    if not raw:
        raise ValueError("PushFrame 原始数据为空")

    frame = PushFrame().parse(raw)
    payload = frame.payload or b""
    if not payload:
        return DecodedFrame(log_id=frame.log_id, need_ack=False, internal_ext="", messages=[])

    try:
        decompressed = gzip.decompress(payload)
    except OSError as exc:  # gzip 头不正确等
        raise ValueError(f"PushFrame.payload gzip 解压失败: {exc}") from exc

    response = Response().parse(decompressed)
    return DecodedFrame(
        log_id=frame.log_id,
        need_ack=bool(response.need_ack),
        internal_ext=str(response.internal_ext or ""),
        messages=list(response.messages_list or []),
    )


def build_heartbeat_frame() -> bytes:
    """构造一帧抖音心跳 ``PushFrame``。

    抖音 Web 协议要求每 5 秒发一帧 ``payload_type='hb'`` 的 ``PushFrame``，
    用来维持长连。
    """
    return bytes(PushFrame(payload_type="hb").SerializeToString())


def build_ack_frame(log_id: int, internal_ext: str) -> bytes:
    """构造一帧 ack ``PushFrame``，用于回执 ``need_ack`` 的消息。

    Args:
        log_id: 原帧的 log_id（必须原样回传）。
        internal_ext: 原 ``Response.internal_ext``，作为 ack 的 payload。
    """
    return bytes(
        PushFrame(
            log_id=log_id,
            payload_type="ack",
            payload=internal_ext.encode("utf-8"),
        ).SerializeToString()
    )


__all__ = [
    "ChatMessage",
    "Common",
    "ControlMessage",
    "DecodedFrame",
    "EmojiChatMessage",
    "FansclubMessage",
    "GiftMessage",
    "Image",
    "LikeMessage",
    "MemberMessage",
    "Message",
    "PushFrame",
    "Response",
    "RoomMessage",
    "RoomRankMessage",
    "RoomStatsMessage",
    "RoomStreamAdaptationMessage",
    "RoomUserSeqMessage",
    "SocialMessage",
    "User",
    "build_ack_frame",
    "build_heartbeat_frame",
    "decode_push_frame",
]
