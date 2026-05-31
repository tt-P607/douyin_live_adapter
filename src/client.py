"""douyin_live_adapter WebSocket 客户端。

负责：

- 拼接抖音 Web 长连 URL（带签名）。
- 建立异步 WebSocket 连接，附 ``ttwid`` Cookie 与 UA。
- 周期性发送 ``payload_type='hb'`` 心跳。
- 持续接收 ``PushFrame`` 二进制帧，解 gzip 后回调到上层。
- 当 ``Response.need_ack`` 为真时立即回 ack 帧。
- 主动 / 被动断连时干净地清理后台任务。

设计要点（与 :mod:`plugins.bilibili_live_adapter.src.client` 思路一致）：

1. **关闭 ws 自带 keepalive**：抖音协议本身有应用层心跳（5s），ws 层 ping 没必要，
   而且长时间阻塞主线程时 ws ping 超时会误杀刚建好的连接。
2. **后台任务统一通过 ``task_manager``**：避免裸 ``asyncio.create_task``。
3. **回调以 ``Message`` 为粒度上抛**：解码层（``proto/__init__.py``）已经把
   ``PushFrame -> Response -> messages`` 的过程封装好；这里只把 ``method`` +
   ``payload`` 喂给上层 dispatcher。
"""

from __future__ import annotations

import asyncio
import random
import string
from collections.abc import Awaitable, Callable
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.kernel.concurrency import get_task_manager

from .proto import (
    Message,
    build_ack_frame,
    build_heartbeat_frame,
    decode_push_frame,
)
from .signature import DouyinSigner, generate_ms_token


logger = get_logger("douyin_live_adapter.client")


# 抖音 Web 长连 URL 的默认 host；同上游项目固定为 lq 节点。
_DEFAULT_WSS_HOST = "wss://webcast100-ws-web-lq.douyin.com"

# 长连 URL 模板的"非签名相关参数"。``room_id`` 与 ``user_unique_id`` 由调用方
# 在运行时替换；其它参数保持上游 ``DouyinLiveWebFetcher.liveMan`` 中的实测值，
# 任何变更都需要同步更新签名算法可能用到的字段。
_BASE_QUERY_PARAMS: dict[str, str] = {
    "app_name": "douyin_web",
    "version_code": "180800",
    "webcast_sdk_version": "1.0.14-beta.0",
    "update_version_code": "1.0.14-beta.0",
    "compress": "gzip",
    "device_platform": "web",
    "cookie_enabled": "true",
    "screen_width": "1536",
    "screen_height": "864",
    "browser_language": "zh-CN",
    "browser_platform": "Win32",
    "browser_name": "Mozilla",
    "browser_version": (
        "5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36%20"
        "(KHTML,%20like%20Gecko)%20Chrome/126.0.0.0%20Safari/537.36"
    ),
    "browser_online": "true",
    "tz_name": "Asia/Shanghai",
    "host": "https://live.douyin.com",
    "aid": "6383",
    "live_id": "1",
    "did_rule": "3",
    "endpoint": "live_pc",
    "support_wrds": "1",
    "im_path": "/webcast/im/fetch/",
    "identity": "audience",
    "need_persist_msg_count": "15",
    "insert_task_id": "",
    "live_reason": "",
    "heartbeatDuration": "0",
}


# 业务消息回调签名：``(method, payload_bytes) -> Awaitable[None]``。
MessageCallback = Callable[[str, bytes], Awaitable[None]]


class DouyinClientError(RuntimeError):
    """抖音 WebSocket 客户端运行期错误。"""


def _generate_user_unique_id() -> str:
    """生成一个伪造的 ``user_unique_id``。

    抖音 Web 端用 19 位数字作为 ``user_unique_id``；本插件作为"匿名观众"接入，
    给一个稳定但伪随机的值即可。
    """

    return "73" + "".join(random.choices(string.digits, k=17))


def _build_cursor() -> str:
    """构造长连初始 ``cursor`` 字段。"""

    # ``cursor`` 在断线重连时应当带上服务器最后给的值；首次连接给一个简单的
    # 占位串即可（与上游保持一致）。
    return "d-1_u-1_fh-7392091211001140287_t-1721106114633_r-1"


def _build_internal_ext(room_id: str, user_unique_id: str) -> str:
    """构造长连初始 ``internal_ext`` 字段。"""

    return (
        f"internal_src:dim|wss_push_room_id:{room_id}|wss_push_did:{user_unique_id}"
        "|first_req_ms:1721106114541|fetch_time:1721106114633|seq:1"
        "|wss_info:0-1721106114633-0-0|wrds_v:7392094459690748497"
    )


class DouyinClient:
    """单次会话的抖音 Web WebSocket 客户端。

    生命周期：

    .. code-block:: text

        client = DouyinClient(signer, on_message=...)
        await client.start(room_id=..., ttwid=..., user_agent=...)
        await client.wait_closed()   # 阻塞直到长连退出
        await client.stop()           # 主动停止时使用

    重连应通过新建一个实例完成（避免状态残留）。
    """

    def __init__(
        self,
        *,
        signer: DouyinSigner,
        on_message: MessageCallback,
        heartbeat_interval: float = 5.0,
        signature_retry_max: int = 3,
        wss_host: str = _DEFAULT_WSS_HOST,
    ) -> None:
        """初始化客户端。

        Args:
            signer: 已构造好的签名器。
            on_message: 业务消息回调；每条 ``Message`` 会被调用一次。
            heartbeat_interval: 心跳发送间隔（秒），抖音建议 5。
            signature_retry_max: 签名失败重试次数。
            wss_host: WebSocket 服务端 host；默认 lq 节点，调试可覆盖。
        """

        self._signer = signer
        self._on_message = on_message
        self._heartbeat_interval = float(heartbeat_interval)
        self._signature_retry_max = int(signature_retry_max)
        self._wss_host = wss_host

        # 运行时状态
        self._ws: Any | None = None
        self._stopping: bool = False
        self._closed_event: asyncio.Event = asyncio.Event()

        # 后台任务句柄（task_manager 返回的 TaskInfo）
        self._recv_task_info: Any | None = None
        self._hb_task_info: Any | None = None

    # ── 公共属性 ─────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        """当前 ws 是否已连接（未关闭）。"""

        return self._ws is not None and not getattr(self._ws, "closed", True)

    async def wait_closed(self) -> None:
        """阻塞等待长连结束。"""

        await self._closed_event.wait()

    # ── 启动 / 停止 ───────────────────────────────────

    async def start(
        self,
        *,
        room_id: str,
        ttwid: str,
        user_agent: str,
    ) -> None:
        """建立长连、跑心跳与 recv 循环。

        Args:
            room_id: 真实 ``room_id``（来自 :meth:`DouyinApi.get_room_id`）。
            ttwid: 抖音首页拿到的 ``ttwid`` Cookie。
            user_agent: 与 HTTP 请求一致的 User-Agent。

        Raises:
            DouyinClientError: 签名失败、ws 连接失败或 ``websockets`` 依赖缺失。
        """

        if not room_id:
            raise DouyinClientError("room_id 不能为空")
        if not ttwid:
            raise DouyinClientError("ttwid 不能为空")

        self._stopping = False
        self._closed_event.clear()

        # 1) 拼 query 参数（不含 signature）
        user_unique_id = _generate_user_unique_id()
        query_params: dict[str, str] = dict(_BASE_QUERY_PARAMS)
        query_params.update(
            {
                "room_id": str(room_id),
                "user_unique_id": user_unique_id,
                "cursor": _build_cursor(),
                "internal_ext": _build_internal_ext(str(room_id), user_unique_id),
            }
        )
        # 抖音签名算法不依赖 ``msToken``，但加上更像正常浏览器流量。
        query_params["msToken"] = generate_ms_token()

        base_url = f"{self._wss_host}/webcast/im/push/v2/?" + "&".join(
            f"{k}={v}" for k, v in query_params.items()
        )

        # 2) 跑签名，签名失败直接抛错（由上层退避重连）
        signature = self._signer.gen_signature(
            base_url, retry_max=self._signature_retry_max
        )
        if not signature:
            raise DouyinClientError(
                "抖音签名计算失败（连续重试均返回空），无法建立长连"
            )
        wss_url = f"{base_url}&signature={signature}"

        logger.info(f"连接抖音长连：room_id={room_id} user_unique_id={user_unique_id}")

        # 3) 建立 ws；需要把 cookie 与 UA 放进 header，否则握手会被拒
        try:
            from websockets.legacy import client as ws_client  # type: ignore
        except ImportError as exc:
            raise DouyinClientError(
                "未安装 websockets 库；请检查 manifest.json 的 python_dependencies"
            ) from exc

        headers = [
            ("Cookie", f"ttwid={ttwid}"),
            ("User-Agent", user_agent),
        ]

        try:
            # 关闭 ws 自带 keepalive（同 B 站插件，原因见模块顶部 docstring）。
            self._ws = await ws_client.connect(
                wss_url,
                extra_headers=headers,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=10,
                max_size=2**24,  # 16MB；抖音 PushFrame 偶有大包
            )
        except Exception as exc:
            raise DouyinClientError(f"WebSocket 连接失败: {exc}") from exc

        logger.info("抖音 WebSocket 连接成功")

        # 4) 启动后台任务
        tm = get_task_manager()
        self._recv_task_info = tm.create_task(
            self._recv_loop(),
            name="douyin_live_adapter.recv",
            daemon=True,
        )
        self._hb_task_info = tm.create_task(
            self._heartbeat_loop(),
            name="douyin_live_adapter.heartbeat",
            daemon=True,
        )

    async def stop(self) -> None:
        """主动停止：取消心跳 / recv，关 ws。"""

        if self._stopping:
            return
        self._stopping = True

        tm = get_task_manager()
        for info in (self._hb_task_info, self._recv_task_info):
            if info is None:
                continue
            try:
                tm.cancel_task(info.task_id)
            except Exception:
                pass
        self._hb_task_info = None
        self._recv_task_info = None

        await self._safe_close_ws()
        self._closed_event.set()
        logger.info("抖音 WebSocket 已关闭")

    # ── 内部：心跳循环 ───────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """每 ``heartbeat_interval`` 秒发一帧抖音 hb 包。

        心跳失败时退出循环，由上层（plugin）感知 ws 断连后触发重连。
        """

        while not self._stopping:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                if self._stopping or self._ws is None:
                    break
                await self._ws.send(build_heartbeat_frame())
                logger.debug("抖音心跳已发送")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(f"抖音心跳异常: {exc}")
                break

    # ── 内部：接收循环 ───────────────────────────────

    async def _recv_loop(self) -> None:
        """持续读 ws 帧，解码后路由到 :attr:`_on_message` 回调。"""

        try:
            async for frame in self._ws:  # type: ignore[union-attr]
                if self._stopping:
                    break
                if not isinstance(frame, (bytes, bytearray)):
                    logger.debug(
                        f"忽略非二进制帧 type={type(frame).__name__}"
                    )
                    continue
                await self._handle_frame(bytes(frame))
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning(f"抖音 recv loop 退出: {exc}")
        finally:
            # ws 自然结束（被服务端关 / 网络断）时也要让 wait_closed 解阻。
            self._closed_event.set()

    async def _handle_frame(self, raw: bytes) -> None:
        """解码一帧 ``PushFrame``，回 ack 并把每条 ``Message`` 喂给回调。"""

        try:
            decoded = decode_push_frame(raw)
        except Exception as exc:
            logger.warning(f"PushFrame 解码失败: {exc}")
            return

        if decoded.need_ack and self._ws is not None:
            try:
                await self._ws.send(
                    build_ack_frame(decoded.log_id, decoded.internal_ext)
                )
            except Exception as exc:
                logger.warning(f"发送 ack 帧失败: {exc}")

        for message in decoded.messages:
            method = str(message.method or "")
            payload = bytes(message.payload or b"")
            if not method or not payload:
                continue
            try:
                await self._on_message(method, payload)
            except Exception as exc:
                # 单条业务消息处理失败不应中断整个长连。
                logger.warning(f"处理 {method} 消息异常（已忽略）: {exc}")

    # ── 内部：关 ws 帮手 ─────────────────────────────

    async def _safe_close_ws(self) -> None:
        """关 ws；任何异常只记日志。"""

        if self._ws is None:
            return
        try:
            await self._ws.close()
        except Exception as exc:
            logger.debug(f"关闭 ws 异常: {exc}")
        self._ws = None


# 暴露 Message 给上层，避免它们再 import proto。
__all__ = [
    "DouyinClient",
    "DouyinClientError",
    "Message",
    "MessageCallback",
]
