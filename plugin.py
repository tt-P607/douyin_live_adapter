"""douyin_live_adapter 插件主入口。

把 :mod:`src.api`、:mod:`src.client`、:mod:`src.dispatcher` 三个分层组件粘合
为一个完整的入站适配器：

- 入站：长连捕获弹幕 / 礼物 / 关注 → ``MessageEnvelope`` → 核心。
- 出站：no-op（抖音 Web 协议不允许第三方发弹幕）。
- 状态：累计点赞 + 在线人数定期写到 ``system_reminder``，给模型 prompt 用。

为什么不用 ``WebSocketAdapterOptions`` 让基类自动管 ws：

抖音 Web 协议有签名 + gzip + 自定义 PushFrame 这一整套自定义二进制流程，
``mofox_wire`` 的自动传输层没有钩子可挂。所以本类完全自管 ws：自己开 ws、
自己跑心跳、自己解码包，完成后再调 ``self.core_sink.send(envelope)`` 把消息
送进核心；这套思路与 :class:`plugins.bilibili_live_adapter.plugin.BilibiliLiveAdapter`
完全一致。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

from mofox_wire import CoreSink, MessageEnvelope

from src.app.plugin_system.api import prompt_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseAdapter, BasePlugin, register_plugin
from src.core.prompt import SystemReminderBucket, SystemReminderInsertType
from src.kernel.concurrency import get_task_manager

from .config import DouyinLiveAdapterConfig
from .src.api import DouyinApi, DouyinApiError
from .src.client import DouyinClient, DouyinClientError
from .src.dispatcher import (
    CONTROL_LIVE_ENDED,
    PLATFORM,
    DouyinDispatcher,
)


logger = get_logger("douyin_live_adapter")


# 直播间状态 system_reminder 名称（点赞数 + 在线人数汇总到这一条）。
_LIKES_REMINDER_NAME = "douyin_live_room_status"

# 状态 reminder 刷新间隔（秒）。抖音点赞推送高频，但 prompt 不需要实时刷新。
_LIKES_REFRESH_INTERVAL = 3.0


class DouyinLiveAdapter(BaseAdapter):
    """抖音直播弹幕入站适配器。"""

    adapter_name = "douyin_live_adapter"
    adapter_version = "0.1.0"
    adapter_author = "Zoo"
    adapter_description = (
        "抖音 Web 协议直播弹幕入站适配器（只入不出，回应交给 anima_chatter）"
    )
    platform = PLATFORM

    # 真实来源平台标识；anima_chatter 等下游通过这个属性拿到本 adapter 真实
    # 投递的源平台（platform 已经被合并为统一虚拟值 ``"live"``）。
    source_platform = "douyin_live"

    run_in_subprocess = False

    def __init__(
        self,
        core_sink: CoreSink,
        plugin: "DouyinLiveAdapterPlugin | None" = None,
        **kwargs: Any,
    ) -> None:
        """不传 transport：自管 ws 长连。"""

        super().__init__(core_sink, plugin=plugin, **kwargs)

        # 运行时资源（on_adapter_loaded 时构造）
        self._api: DouyinApi | None = None
        self._client: DouyinClient | None = None
        self._dispatcher: DouyinDispatcher | None = None

        # 自管的会话循环（负责 get_room_id → client.start → 等断 → 重连）
        self._session_task_info: Any | None = None
        self._stopping: bool = False

        # 重连指数退避状态
        self._consecutive_failures: int = 0

        # 当被 ControlMessage(status=3) 触发停止时设置；session_loop 据此跳过重连。
        self._live_ended: bool = False

        # 缓存当前会话信息
        self._current_room_id: str = ""
        self._cached_ttwid: str = ""

        # 状态 reminder 刷新任务
        self._reminder_task_info: Any | None = None
        self._last_published_likes: int = -1
        self._last_published_viewers: int = -1

    # ── 配置读取 ──────────────────────────────────────

    def _get_config(self) -> DouyinLiveAdapterConfig:
        """拿到本插件的配置；缺失时直接抛错（不允许带空 live_id 启动）。"""

        if self.plugin is None or self.plugin.config is None:
            raise RuntimeError("DouyinLiveAdapter 启动失败：插件配置缺失")
        config = cast(DouyinLiveAdapterConfig, self.plugin.config)
        if not config.douyin.live_id:
            raise RuntimeError(
                "抖音直播间未配置：config.douyin.live_id 不能为空"
            )
        return config

    def _is_plugin_enabled(self) -> bool:
        """读取 ``[plugin].enabled`` 开关；缺配置时视为"未启用"。"""

        if self.plugin is None or self.plugin.config is None:
            return False
        config = cast(DouyinLiveAdapterConfig, self.plugin.config)
        return bool(config.plugin.enabled)

    # ── 生命周期 ──────────────────────────────────────

    async def on_adapter_loaded(self) -> None:
        """构造 HTTP API + dispatcher，但不在这里建立长连。

        长连放到 ``start()`` 之后的会话循环里，由 :meth:`BaseAdapter.start`
        触发。

        若 ``[plugin].enabled = false``，本钩子直接 no-op：不读 ``live_id``，
        不构造 HTTP/dispatcher，等价于"框架知道这个插件存在，但它什么都不做"。
        """

        if not self._is_plugin_enabled():
            logger.info("[plugin].enabled = false，抖音 Adapter 已禁用（不会建立长连）")
            return

        config = self._get_config()

        self._api = DouyinApi(
            user_agent=config.douyin.user_agent,
            timeout=float(config.connection.request_timeout),
            cookie=config.douyin.cookie,
        )
        self._dispatcher = DouyinDispatcher(
            stream_name_override=config.douyin.stream_name,
        )
        logger.info("抖音 Adapter 配置就绪，等待 start() 建立长连")

    async def on_adapter_unloaded(self) -> None:
        """关闭 client / api / dispatcher 等运行时资源。"""

        await self._stop_session()
        self._cancel_reminder_task()
        self._clear_reminder_entry()

        if self._api is not None:
            try:
                await self._api.aclose()
            except Exception as exc:
                logger.warning(f"关闭 HTTP 客户端异常: {exc}")
            self._api = None

        # 释放签名器持有的 V8 上下文
        signer = getattr(self, "_signer", None)
        if signer is not None:
            try:
                signer.close()
            except Exception as exc:
                logger.debug(f"关闭签名器异常: {exc}")

        self._dispatcher = None
        logger.info("抖音 Adapter 已卸载")

    async def start(self) -> None:
        """启动 BaseAdapter 公共流程 + 自管会话循环。

        ``[plugin].enabled = false`` 时只走基类启动流程，不启动会话/reminder
        任务——adapter 仍然在 ``adapter_manager`` 里登记，但不消耗 WS 资源。
        """

        await super().start()

        if not self._is_plugin_enabled():
            return

        self._stopping = False
        self._live_ended = False
        self._consecutive_failures = 0
        self._last_published_likes = -1
        self._last_published_viewers = -1

        tm = get_task_manager()
        self._session_task_info = tm.create_task(
            self._session_loop(),
            name="douyin_live_adapter.session",
            daemon=True,
        )
        self._reminder_task_info = tm.create_task(
            self._reminder_loop(),
            name="douyin_live_adapter.reminder",
            daemon=True,
        )

    async def stop(self) -> None:
        """先停自家会话循环，再走 BaseAdapter 公共停止。"""

        self._stopping = True
        await self._stop_session()
        self._cancel_reminder_task()
        self._clear_reminder_entry()
        await super().stop()

    # ── 健康检查（重写：不要看 self._ws） ─────────────

    async def health_check(self) -> bool:
        """返回当前会话是否健康。

        与 B 站插件相同的逻辑：基类默认看 ``self._ws``，但我们自管 ws，必须
        重写——否则基类巡检会一直认为没连上，狂调 ``reconnect()``。
        """

        if self._stopping or self._live_ended:
            return True
        if self._client is None:
            return False
        return self._client.is_connected

    async def reconnect(self) -> None:
        """基类健康检查不通过时会调这个；本 adapter 自管重连。"""

        logger.debug("基类 reconnect 被触发，但本 adapter 自管重连，已忽略")

    # ── 出站：no-op ───────────────────────────────────

    async def _send_platform_message(self, envelope: MessageEnvelope) -> None:  # type: ignore[override]
        """抖音 Web 协议不允许 Bot 发弹幕，整体丢弃出站。"""

        seg = envelope.get("message_segment")
        snippet: str
        if isinstance(seg, dict) and seg.get("type") == "text":
            snippet = str(seg.get("data") or "")[:30]
        else:
            snippet = ""
        logger.debug(f"忽略抖音出站消息（平台不允许 Bot 发弹幕）: {snippet}")

    # ── 入站：被 client 回调直接喂给 dispatcher ───────

    async def from_platform_message(self, raw: Any) -> MessageEnvelope | None:  # type: ignore[override]
        """本 adapter 自管 ws；client 回调直接走 :meth:`_on_client_message`。

        基类要求实现这个抽象方法，但实际入站路径不会经过它。保留兼容空壳。
        """

        return None

    async def get_bot_info(self) -> dict[str, Any]:  # type: ignore[override]
        """返回 Bot 在该平台上的身份信息（即"主播"）。"""

        anchor_uname = ""
        if self._dispatcher is not None:
            anchor_uname = self._dispatcher.anchor_uname
        return {
            "bot_id": self._current_room_id or "0",
            "bot_name": anchor_uname or "抖音主播",
            "platform": self.platform,
        }

    # ── 会话循环：get_room_id → client.start → 等断 → 退避重连 ──

    async def _session_loop(self) -> None:
        """长跑任务：维持一次"建立长连 → 跑心跳 → 断了重连"会话。"""

        while not self._stopping and not self._live_ended:
            try:
                await self._run_one_session()
                if self._stopping or self._live_ended:
                    break
            except asyncio.CancelledError:
                break
            except (DouyinApiError, DouyinClientError) as exc:
                logger.warning(f"抖音会话异常: {exc}")
            except Exception as exc:
                logger.error(f"抖音会话未预期异常: {exc}", exc_info=True)

            if self._stopping or self._live_ended:
                break
            if not self._auto_reconnect_enabled():
                logger.info("auto_reconnect 已关闭，退出会话循环")
                break

            await self._sleep_with_backoff()

        logger.info("抖音会话循环退出")

    async def _run_one_session(self) -> None:
        """完整跑一次会话：拿 ttwid + room_id → client.start → 等到 client 退出。"""

        if self._api is None or self._dispatcher is None:
            raise RuntimeError(
                "API / dispatcher 尚未初始化（on_adapter_loaded 没跑过？）"
            )

        config = self._get_config()
        live_id = config.douyin.live_id

        # 1) 拿 ttwid + room_id
        ttwid = await self._api.get_ttwid()
        room_id = await self._api.get_room_id(live_id, ttwid=ttwid)
        self._cached_ttwid = ttwid
        self._current_room_id = room_id
        self._dispatcher.update_room_context(room_id=room_id)
        logger.info(f"抖音房间信息就绪 live_id={live_id} room_id={room_id}")

        # 2) 构造签名器（懒加载 JS，不会立即吃 V8 内存）
        signer = self._ensure_signer()

        # 3) 构造 client
        self._client = DouyinClient(
            signer=signer,
            on_message=self._on_client_message,
            heartbeat_interval=float(config.connection.heartbeat_interval),
            signature_retry_max=int(config.signature.retry_max),
        )

        try:
            # 4) 建立长连 + 启动 recv / 心跳
            await self._client.start(
                room_id=room_id,
                ttwid=ttwid,
                user_agent=config.douyin.user_agent,
            )
            self._consecutive_failures = 0

            # 5) 等 client 自己结束
            await self._client.wait_closed()
        finally:
            try:
                await self._client.stop()
            except Exception as exc:
                logger.debug(f"停止 client 异常: {exc}")
            self._client = None

    async def _stop_session(self) -> None:
        """关闭当前会话与会话循环。"""

        if self._client is not None:
            try:
                await self._client.stop()
            except Exception as exc:
                logger.debug(f"关闭 client 异常: {exc}")

        if self._session_task_info is not None:
            tm = get_task_manager()
            try:
                tm.cancel_task(self._session_task_info.task_id)
            except Exception:
                pass
            self._session_task_info = None

        self._client = None

    def _auto_reconnect_enabled(self) -> bool:
        """配置开关：是否允许长连断开后自动重连。"""

        try:
            config = self._get_config()
        except Exception:
            return False
        return bool(config.connection.auto_reconnect)

    async def _sleep_with_backoff(self) -> None:
        """指数退避：连续失败次数越多，等待越久（封顶 ``reconnect_max_delay``）。"""

        config = self._get_config()
        conn = config.connection
        self._consecutive_failures += 1
        delay = float(conn.reconnect_initial_delay) * (
            2 ** (self._consecutive_failures - 1)
        )
        delay = min(delay, float(conn.reconnect_max_delay))
        logger.info(
            f"等待 {delay:.1f}s 后重连（连续失败 {self._consecutive_failures} 次）"
        )
        await asyncio.sleep(delay)

    # ── 签名器懒加载 ─────────────────────────────────

    def _ensure_signer(self):  # noqa: ANN202 - 内部辅助
        """懒加载签名器；JS 资源相对插件目录定位。"""

        signer = getattr(self, "_signer", None)
        if signer is not None:
            return signer

        from .src.signature import DouyinSigner

        assets_dir = Path(__file__).resolve().parent / "assets"
        signer = DouyinSigner(assets_dir=assets_dir)
        self._signer = signer
        return signer

    # ── client 业务消息回调 ──────────────────────────

    async def _on_client_message(self, method: str, payload: bytes) -> None:
        """从 :class:`DouyinClient` 接收到一条业务消息。"""

        if self._dispatcher is None:
            return

        result = await self._dispatcher.dispatch(method, payload)

        if result.signal == CONTROL_LIVE_ENDED:
            logger.info("主播下播，停止重连")
            self._live_ended = True
            # 触发 client 退出 wait_closed
            if self._client is not None:
                try:
                    await self._client.stop()
                except Exception as exc:
                    logger.debug(f"主动关 client 异常: {exc}")
            return

        if result.envelope is not None:
            try:
                await self.core_sink.send(result.envelope)
            except Exception as exc:
                logger.warning(f"投递消息到 core_sink 失败: {exc}")

    # ── system_reminder 周期刷新 ─────────────────────

    async def _reminder_loop(self) -> None:
        """周期性把 dispatcher 的累计点赞 + 在线人数刷到 system_reminder.actor。"""

        try:
            while not self._stopping:
                try:
                    await asyncio.sleep(_LIKES_REFRESH_INTERVAL)
                except asyncio.CancelledError:
                    raise
                if self._stopping:
                    break
                self._publish_reminder()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(f"reminder 循环异常退出: {exc}")

    def _publish_reminder(self) -> None:
        """把当前点赞数 + 直播间号 + 在线人数写到 system_reminder.actor。

        - dispatcher 未就绪 / 还没建立会话 → 跳过。
        - 数值与上次相同 → 跳过（避免日志噪音 + store 反复刷写）。
        """

        dispatcher = self._dispatcher
        if dispatcher is None:
            return
        likes = dispatcher.total_likes
        viewers = dispatcher.current_viewers
        room_id = dispatcher.room_id

        if likes == self._last_published_likes and viewers == self._last_published_viewers:
            return

        parts: list[str] = []
        if room_id:
            parts.append(f"当前抖音直播间号 {room_id}")
        parts.append(f"本次开播至今观众已累计点赞 {likes} 次")
        if viewers > 0:
            parts.append(f"当前在线观众约 {viewers} 人")
        content = "，".join(parts) + "。"

        try:
            prompt_api.add_system_reminder(
                bucket=SystemReminderBucket.ACTOR,
                name=_LIKES_REMINDER_NAME,
                content=content,
                insert_type=SystemReminderInsertType.DYNAMIC,
            )
        except Exception as exc:
            logger.debug(f"写入 reminder 失败（忽略）: {exc}")
            return
        self._last_published_likes = likes
        self._last_published_viewers = viewers

    def _cancel_reminder_task(self) -> None:
        """取消 reminder 刷新任务（stop 时调）。"""

        if self._reminder_task_info is None:
            return
        tm = get_task_manager()
        try:
            tm.cancel_task(self._reminder_task_info.task_id)
        except Exception:
            pass
        self._reminder_task_info = None

    def _clear_reminder_entry(self) -> None:
        """卸载时把 system_reminder 里的状态条目清掉。"""

        try:
            from src.core.prompt import get_system_reminder_store

            store = get_system_reminder_store()
            store.delete(SystemReminderBucket.ACTOR, _LIKES_REMINDER_NAME)
        except Exception as exc:
            logger.debug(f"清理 reminder 失败（忽略）: {exc}")


@register_plugin
class DouyinLiveAdapterPlugin(BasePlugin):
    """抖音直播弹幕适配器插件。"""

    plugin_name = "douyin_live_adapter"
    plugin_version = "0.1.0"
    plugin_author = "Zoo"
    plugin_description = "基于 Web 协议的抖音直播弹幕入站适配器（基于 Neo-MoFox）"
    configs = [DouyinLiveAdapterConfig]

    def get_components(self) -> list[type]:
        """返回插件内所有组件类。"""

        return [DouyinLiveAdapter]


__all__ = [
    "DouyinLiveAdapter",
    "DouyinLiveAdapterPlugin",
]
