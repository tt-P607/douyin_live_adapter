"""douyin_live_adapter 业务消息分发器。

把抖音 ``Message.method`` + ``payload`` 翻译成统一的 :class:`MessageEnvelope`。

支持的事件：

- ``WebcastChatMessage`` 弹幕 → 文本消息。
- ``WebcastGiftMessage`` 礼物 → 描述性文本（``[送出礼物] xxx ×N``）。
- ``WebcastSocialMessage`` 关注主播 → 描述性文本（``[关注主播]``）。
- ``WebcastMemberMessage`` 进入直播间 → 描述性文本（``[进场]``，可选）。
- ``WebcastLikeMessage`` 点赞 → 不下发 envelope，累加 :attr:`total_likes`。
- ``WebcastRoomUserSeqMessage`` 在线统计 → 不下发 envelope，更新 :attr:`current_viewers`
  / :attr:`total_pv_for_anchor`。
- ``WebcastControlMessage`` 直播状态 → status==3 时返回 ``"__live_ended__"``，由上层
  停止重连。

设计要点：

- 平台标识固定 ``platform = "live"``（合并多平台直播 stream 用的虚拟值；与
  ``bilibili_live_adapter`` 对齐）。真实来源 ``"douyin_live"`` 通过
  ``additional_config.source_platform`` 暴露。
- 抖音用户 ID 直接用 ``User.id_str``（``uint64`` 字符串），缺失退化到 ``User.id``
  / ``sec_uid``。
- 群上下文：``group_id = "live_room"``（合并值；多平台共用一个 stream），
  group name 仍写 ``"抖音直播间 {room_id}"``，真实 room_id 通过
  ``additional_config.source_room_id`` 暴露。
- 礼物 / 关注 / 进场等"非纯文本"事件复用 DM 那套 builder，``message_segment`` 顶层
  仍然是 ``text``，只是文本带 ``[标签]`` 前缀，让模型一眼能区分。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mofox_wire import MessageBuilder, MessageEnvelope
from mofox_wire.types import UserRole

from src.app.plugin_system.api.log_api import get_logger

from .proto import (
    ChatMessage,
    ControlMessage,
    GiftMessage,
    LikeMessage,
    MemberMessage,
    RoomUserSeqMessage,
    SocialMessage,
    User,
)


logger = get_logger("douyin_live_adapter.dispatcher")


# 合并 stream 后的虚拟平台名；与 :class:`plugin.DouyinLiveAdapter.platform`
# 一致。把所有直播平台（B 站 / 抖音 / 未来的 YouTube 等）都标成 ``"live"``，
# 让多平台同播时 chat_stream 共用一个，避免 chatter 打架。
PLATFORM = "live"

# 真实来源平台标识；写到 envelope 的 ``additional_config.source_platform``，
# 让 prompt 能告诉模型这条弹幕到底来自哪。
SOURCE_PLATFORM = "douyin_live"

# 合并后的虚拟 group_id。所有直播平台共用这一个值，使得 stream_manager 通过
# ``SHA256(platform + "_" + group_id)`` 算出来的 stream_id 在 B 站 + 抖音之间
# 完全一致，进入同一会话。
LIVE_VIRTUAL_GROUP_ID = "live_room"


# ── 已知 method 常量 ─────────────────────────────────
METHOD_CHAT = "WebcastChatMessage"
METHOD_GIFT = "WebcastGiftMessage"
METHOD_LIKE = "WebcastLikeMessage"
METHOD_MEMBER = "WebcastMemberMessage"
METHOD_SOCIAL = "WebcastSocialMessage"
METHOD_ROOM_USER_SEQ = "WebcastRoomUserSeqMessage"
METHOD_CONTROL = "WebcastControlMessage"


# 控制消息约定的"特殊返回值"——不是真正的 envelope，但 plugin 层会识别它来
# 决定是否停止重连。用类型而不是字符串 sentinel 也行，但字符串更便于日志。
CONTROL_LIVE_ENDED = "__live_ended__"


@dataclass
class DispatchResult:
    """分发结果。

    ``envelope`` 与 ``signal`` 互斥：

    - 普通业务消息 → ``envelope`` 非空，``signal`` 为 ``None``。
    - 点赞 / 在线统计这类只更新内部计数的事件 → 两者都为 ``None``。
    - ``WebcastControlMessage`` status=3 下播 → ``signal=CONTROL_LIVE_ENDED``。
    """

    envelope: MessageEnvelope | None = None
    signal: str | None = None


class DouyinDispatcher:
    """把抖音业务 ``method`` 路由成 ``MessageEnvelope``。

    构造时只记录会话上下文（房间号、主播昵称等），运行时通过
    :meth:`update_room_context` 更新；点赞 / 在线统计等只累计到内部状态，由
    :class:`plugin.DouyinLiveAdapter` 周期性把这些数据注入 prompt 的
    ``system_reminder``。
    """

    def __init__(
        self,
        *,
        room_id: str = "",
        anchor_uname: str = "",
        emit_member_events: bool = False,
        stream_name_override: str = "",
    ) -> None:
        """初始化分发器。

        Args:
            room_id: 真实直播间号，会作为群 ID。
            anchor_uname: 主播昵称（备日志 / bot_info 使用）。
            emit_member_events: 是否把 ``WebcastMemberMessage`` 进场事件转为
                envelope；默认关闭，避免噪音。
            stream_name_override: 用户自定义的 stream_name；非空时直接使用，
                留空时回退到 ``"抖音直播间 {room_id}"`` 兜底。
        """

        self._room_id = str(room_id or "")
        self._anchor_uname = str(anchor_uname or "")
        self._emit_member_events = bool(emit_member_events)
        self._stream_name_override = str(stream_name_override or "").strip()

        # 累计计数（reminder 使用）
        self._total_likes: int = 0
        self._current_viewers: int = 0
        self._total_pv_for_anchor: int = 0

    # ── 上下文 ───────────────────────────────────────

    def update_room_context(self, *, room_id: str, anchor_uname: str = "") -> None:
        """重连后更新房间上下文（不重置点赞累计）。"""

        self._room_id = str(room_id or "")
        if anchor_uname:
            self._anchor_uname = str(anchor_uname)

    def reset_counters(self) -> None:
        """显式清零点赞 / 在线统计；插件 unload / 重置时调。"""

        self._total_likes = 0
        self._current_viewers = 0
        self._total_pv_for_anchor = 0

    @property
    def room_id(self) -> str:
        """当前直播间号。"""

        return self._room_id

    @property
    def anchor_uname(self) -> str:
        """当前主播昵称。"""

        return self._anchor_uname

    @property
    def total_likes(self) -> int:
        """开播至今累计点赞数（每条 ``WebcastLikeMessage.count`` 累加）。"""

        return self._total_likes

    @property
    def current_viewers(self) -> int:
        """当前在线观看人数（来自 ``WebcastRoomUserSeqMessage.total``）。"""

        return self._current_viewers

    @property
    def total_pv_for_anchor(self) -> int:
        """累计观看人数（来自 ``RoomUserSeqMessage.total_pv_for_anchor``）。"""

        return self._total_pv_for_anchor

    # ── 主入口 ───────────────────────────────────────

    async def dispatch(self, method: str, payload: bytes) -> DispatchResult:
        """根据 ``method`` 路由到对应的 ``_parse_xxx`` 处理函数。

        Args:
            method: 抖音业务方法名（如 ``WebcastChatMessage``）。
            payload: 该消息的 Protobuf 二进制 payload。

        Returns:
            :class:`DispatchResult`；具体含义见类 docstring。
        """

        try:
            if method == METHOD_CHAT:
                envelope = self._parse_chat(payload)
                return DispatchResult(envelope=envelope)
            if method == METHOD_GIFT:
                envelope = self._parse_gift(payload)
                return DispatchResult(envelope=envelope)
            if method == METHOD_SOCIAL:
                envelope = self._parse_social(payload)
                return DispatchResult(envelope=envelope)
            if method == METHOD_MEMBER:
                if self._emit_member_events:
                    envelope = self._parse_member(payload)
                    return DispatchResult(envelope=envelope)
                return DispatchResult()
            if method == METHOD_LIKE:
                self._handle_like(payload)
                return DispatchResult()
            if method == METHOD_ROOM_USER_SEQ:
                self._handle_room_user_seq(payload)
                return DispatchResult()
            if method == METHOD_CONTROL:
                return self._handle_control(payload)
        except Exception as exc:
            logger.warning(f"分发 {method} 消息异常: {exc}")
            return DispatchResult()

        logger.debug(f"未知 method 已忽略: {method}")
        return DispatchResult()

    # ── 共享：构造 user / group ──────────────────────

    @staticmethod
    def _resolve_user_id(user: User | None) -> str:
        """从抖音 ``User`` 中拿到稳定 ``user_id``。

        优先级：``sec_uid`` > ``id_str`` > ``id`` > ``"anon"``。

        - ``sec_uid`` 是抖音对外脱敏的"用户身份"，跨直播间稳定，与 B 站
          ``open_id`` 语义等价；**适合作为 person_id 进记忆**。
        - ``id_str`` / ``id`` 是 uint64 形式的"房间会话内"短 ID，每次进出
          直播间可能变，仅作兜底使用。
        """

        if user is None:
            return "anon"
        if user.sec_uid:
            return str(user.sec_uid)
        if user.id_str:
            return str(user.id_str)
        if user.id:
            return str(user.id)
        return "anon"

    @staticmethod
    def _user_avatar_url(user: User | None) -> str:
        """从 ``User`` 中提取一个头像 URL；优先 medium，其次 thumb / large。"""

        if user is None:
            return ""
        for image in (user.avatar_medium, user.avatar_thumb, user.avatar_large):
            if image is None:
                continue
            urls = list(image.url_list_list or [])
            if urls:
                return str(urls[0])
        return ""

    def _apply_user(
        self,
        builder: MessageBuilder,
        *,
        user: User | None,
        force_role: UserRole | None = None,
    ) -> str:
        """把发送者信息塞进 builder，返回 user_id 便于日志。"""

        user_id = self._resolve_user_id(user)
        nickname = str(user.nick_name) if user is not None else ""
        avatar = self._user_avatar_url(user)
        role = force_role or UserRole.MEMBER

        builder.from_user(
            user_id=user_id,
            platform=PLATFORM,
            nickname=nickname,
            user_avatar=avatar,
            role=role,
        )
        return user_id

    def _apply_group(self, builder: MessageBuilder) -> None:
        """把直播间映射成"群"。

        ``group_id`` 写虚拟值 ``"live_room"`` ——多平台直播都共享同一个 stream，
        交给 anima_chatter 串行处理。真实抖音 ``room_id`` 通过 group name 与
        ``additional_config.source_room_id`` 暴露。

        ``group_name`` 优先用配置里的 ``stream_name_override``（用户自取的舞台名）；
        留空时回退到 ``"抖音直播间 {room_id}"`` 兜底。
        """

        if not self._room_id:
            return
        name = self._stream_name_override or f"抖音直播间 {self._room_id}"
        builder.from_group(
            group_id=LIVE_VIRTUAL_GROUP_ID,
            platform=PLATFORM,
            name=name,
        )

    @staticmethod
    def _inject_source_into_extra(envelope: MessageEnvelope, additional: dict[str, Any]) -> None:
        """把 ``source_platform`` / ``source_room_id`` 复制到 ``message_info.extra``。

        ``MessageConverter`` 只把 ``message_info.extra`` 透传到 ``Message.extra``；
        ``additional_config`` 是平台原始字段，**不会**进入 ``Message.extra``。所以
        这里要手动同步——保证 anima_chatter 等下游可以通过 ``msg.extra.get(...)`` 直接
        读到来源信息，不必绕到 ``raw_message`` 或 ``additional_config``。
        """

        info = envelope.get("message_info")
        if not isinstance(info, dict):
            return
        extra_obj = info.get("extra")
        if not isinstance(extra_obj, dict):
            extra_obj = {}
            info["extra"] = extra_obj  # type: ignore[typeddict-unknown-key]
        if "source_platform" in additional:
            extra_obj["source_platform"] = additional["source_platform"]
        if "source_room_id" in additional:
            extra_obj["source_room_id"] = additional["source_room_id"]

    def _build_common_additional(self, user: User | None) -> dict[str, Any]:
        """提取所有事件共有的"平台共享字段"。

        关键字段：
        - ``source_platform``：``"douyin_live"``，让 anima_chatter 等下游
          知道这条弹幕来自抖音（envelope 顶层 ``platform`` 已被合并为 ``"live"``）。
        - ``source_room_id``：真实抖音 ``room_id``（字符串，长 uint64）。
        - ``douyin_sec_uid``：跨房稳定 ID；已被用作 ``user_info.user_id``，
          这里再次保留方便外部直接读取。
        """

        base: dict[str, Any] = {
            "source_platform": SOURCE_PLATFORM,
            "source_room_id": str(self._room_id or ""),
        }
        if user is None:
            base.update(
                {
                    "douyin_sec_uid": "",
                    "douyin_user_id": "",
                    "douyin_short_id": 0,
                }
            )
        else:
            base.update(
                {
                    "douyin_sec_uid": str(user.sec_uid or ""),
                    "douyin_user_id": str(user.id_str or user.id or ""),
                    "douyin_short_id": int(user.short_id or 0),
                }
            )
        return base

    # ── 弹幕 ────────────────────────────────────────

    def _parse_chat(self, payload: bytes) -> MessageEnvelope | None:
        """``WebcastChatMessage`` → 一条文本消息形态的 envelope。"""

        message = ChatMessage().parse(payload)
        content = str(message.content or "").strip()
        if not content:
            return None

        user = message.user
        common = message.common

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(content)
        )
        if common is not None and common.msg_id:
            builder.message_id(str(common.msg_id))
        if common is not None and common.create_time:
            # 抖音的 create_time 是毫秒。
            builder.timestamp_ms(int(common.create_time))

        user_id = self._apply_user(builder, user=user)
        self._apply_group(builder)

        envelope = builder.build()
        additional = self._build_common_additional(user)
        additional.update({"event_type": "danmaku"})
        envelope["message_info"]["additional_config"] = additional
        self._inject_source_into_extra(envelope, additional)

        nickname = str(user.nick_name) if user is not None else ""
        logger.info(
            f"收到弹幕 [room={self._room_id}] {nickname}({user_id}): {content}"
        )
        return envelope

    # ── 礼物 ────────────────────────────────────────

    def _parse_gift(self, payload: bytes) -> MessageEnvelope | None:
        """``WebcastGiftMessage`` → 描述性文本消息。

        文案样式：``[送出礼物] 小心心 ×3``。``combo_count`` 优先，缺失退化到
        ``repeat_count``。
        """

        message = GiftMessage().parse(payload)
        user = message.user
        gift = message.gift

        gift_name = ""
        if gift is not None:
            gift_name = str(gift.name or gift.describe or "")
        if not gift_name:
            gift_name = "未知礼物"

        gift_count = int(message.combo_count or message.repeat_count or 1)
        if gift_count <= 0:
            gift_count = 1

        msg_text = f"[送出礼物] {gift_name} ×{gift_count}"

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )
        common = message.common
        if common is not None and common.msg_id:
            builder.message_id(str(common.msg_id))
        if common is not None and common.create_time:
            builder.timestamp_ms(int(common.create_time))

        user_id = self._apply_user(builder, user=user, force_role=UserRole.OPERATOR)
        self._apply_group(builder)

        envelope = builder.build()
        additional = self._build_common_additional(user)
        additional.update(
            {
                "event_type": "gift",
                "gift_id": int(message.gift_id or 0),
                "gift_name": gift_name,
                "gift_count": gift_count,
                "diamond_count": int(gift.diamond_count) if gift is not None else 0,
            }
        )
        envelope["message_info"]["additional_config"] = additional
        self._inject_source_into_extra(envelope, additional)

        nickname = str(user.nick_name) if user is not None else ""
        logger.info(
            f"收到礼物 [room={self._room_id}] "
            f"{nickname}({user_id}) → {gift_name} ×{gift_count}"
        )
        return envelope

    # ── 关注 ────────────────────────────────────────

    def _parse_social(self, payload: bytes) -> MessageEnvelope | None:
        """``WebcastSocialMessage`` → 描述性文本消息（仅处理 action=1 关注）。"""

        message = SocialMessage().parse(payload)
        # action=1 是关注；其它 action 暂不处理。
        if int(message.action or 0) != 1:
            return None

        user = message.user
        msg_text = "[关注主播]"

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )
        common = message.common
        if common is not None and common.msg_id:
            builder.message_id(str(common.msg_id))
        if common is not None and common.create_time:
            builder.timestamp_ms(int(common.create_time))

        user_id = self._apply_user(builder, user=user)
        self._apply_group(builder)

        envelope = builder.build()
        additional = self._build_common_additional(user)
        additional.update({"event_type": "follow"})
        envelope["message_info"]["additional_config"] = additional
        self._inject_source_into_extra(envelope, additional)

        nickname = str(user.nick_name) if user is not None else ""
        logger.info(f"新关注 [room={self._room_id}] {nickname}({user_id}) 关注了主播")
        return envelope

    # ── 进场 ────────────────────────────────────────

    def _parse_member(self, payload: bytes) -> MessageEnvelope | None:
        """``WebcastMemberMessage`` → 描述性文本消息（默认关闭，可在配置打开）。"""

        message = MemberMessage().parse(payload)
        user = message.user
        msg_text = "[进入直播间]"

        builder = (
            MessageBuilder()
            .direction("incoming")
            .platform(PLATFORM)
            .text(msg_text)
        )
        common = message.common
        if common is not None and common.msg_id:
            builder.message_id(str(common.msg_id))
        if common is not None and common.create_time:
            builder.timestamp_ms(int(common.create_time))

        user_id = self._apply_user(builder, user=user)
        self._apply_group(builder)

        envelope = builder.build()
        additional = self._build_common_additional(user)
        additional.update(
            {
                "event_type": "enter",
                "member_count": int(message.member_count or 0),
            }
        )
        envelope["message_info"]["additional_config"] = additional
        self._inject_source_into_extra(envelope, additional)

        nickname = str(user.nick_name) if user is not None else ""
        logger.debug(f"进场 [room={self._room_id}] {nickname}({user_id})")
        return envelope

    # ── 点赞（不下发 envelope） ──────────────────────

    def _handle_like(self, payload: bytes) -> None:
        """``WebcastLikeMessage`` → 累加点赞计数。"""

        message = LikeMessage().parse(payload)
        increment = int(message.count or 1)
        if increment <= 0:
            increment = 1
        self._total_likes += increment

        user_name = ""
        if message.user is not None:
            user_name = str(message.user.nick_name or "")
        logger.debug(
            f"点赞 +{increment} 累计={self._total_likes} (uname={user_name})"
        )

    # ── 在线人数（不下发 envelope） ─────────────────

    def _handle_room_user_seq(self, payload: bytes) -> None:
        """``WebcastRoomUserSeqMessage`` → 更新当前在线 / 累计观看人数。"""

        message = RoomUserSeqMessage().parse(payload)
        try:
            self._current_viewers = int(message.total or 0)
        except (TypeError, ValueError):
            self._current_viewers = 0
        # ``total_pv_for_anchor`` 是字符串，可能是 ``"1.2万"`` 这种带后缀的；
        # 仅在能直接 int 时记录，否则留空。
        try:
            self._total_pv_for_anchor = int(message.total_pv_for_anchor or 0)
        except (TypeError, ValueError):
            self._total_pv_for_anchor = 0
        logger.debug(
            f"在线统计 current={self._current_viewers} "
            f"total_pv={self._total_pv_for_anchor}"
        )

    # ── 直播状态 ─────────────────────────────────────

    def _handle_control(self, payload: bytes) -> DispatchResult:
        """``WebcastControlMessage`` status=3 下播 → 发出停止信号。"""

        message = ControlMessage().parse(payload)
        status = int(message.status or 0)
        logger.debug(f"收到控制消息 status={status}")
        if status == 3:
            logger.info("收到下播信号 (ControlMessage.status=3)")
            return DispatchResult(signal=CONTROL_LIVE_ENDED)
        return DispatchResult()


__all__ = [
    "CONTROL_LIVE_ENDED",
    "DispatchResult",
    "DouyinDispatcher",
    "LIVE_VIRTUAL_GROUP_ID",
    "METHOD_CHAT",
    "METHOD_CONTROL",
    "METHOD_GIFT",
    "METHOD_LIKE",
    "METHOD_MEMBER",
    "METHOD_ROOM_USER_SEQ",
    "METHOD_SOCIAL",
    "PLATFORM",
    "SOURCE_PLATFORM",
]
