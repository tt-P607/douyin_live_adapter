"""douyin_live_adapter HTTP API 客户端。

封装抖音 Web 版页面/接口请求：

- :meth:`DouyinApi.get_ttwid` 拿到首页 Cookie 中的 ``ttwid``，作为后续
  WebSocket 鉴权 Cookie。
- :meth:`DouyinApi.get_room_id` 从直播间页面 HTML 中正则提取真实 ``room_id``。
- :meth:`DouyinApi.aclose` 关闭底层 httpx 客户端，插件卸载时调用。

抖音 Web 没有公开 API；本模块的所有请求只是模拟浏览器访问公开页面。
"""

from __future__ import annotations

import re

import httpx

from src.app.plugin_system.api.log_api import get_logger

from .signature import generate_ms_token

logger = get_logger("douyin_live_adapter.api")


# 匹配嵌入在 HTML 里的 roomId / room_id_str 字段。
# 抖音前端会用 ``\"`` 转义把 JSON 嵌进 HTML 字符串里，所以正则要匹配
# ``\"key\":\"123\"`` 这种形态。
_ROOM_ID_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r'\\"roomId\\":\\"(\d+)\\"'),
    re.compile(r'\\"room_id_str\\":\\"(\d+)\\"'),
    # 兜底：直接 JSON（未转义）
    re.compile(r'"roomId":"(\d+)"'),
    re.compile(r'"room_id_str":"(\d+)"'),
)


class DouyinApiError(RuntimeError):
    """抖音 API 请求异常。"""


class DouyinApi:
    """抖音 Web 版 HTTP 接口封装。"""

    def __init__(
        self,
        *,
        user_agent: str,
        timeout: float = 10.0,
        cookie: str = "",
    ) -> None:
        """初始化 API 客户端。

        Args:
            user_agent: 模拟浏览器的 User-Agent。
            timeout: 请求超时时间（秒）。
            cookie: 可选的手动 Cookie；用户自行抓的浏览器完整 Cookie 串。
        """
        self._user_agent = user_agent
        self._timeout = timeout
        self._manual_cookie = cookie
        self._cached_ttwid: str | None = None

        self._client = httpx.AsyncClient(
            headers={"User-Agent": self._user_agent},
            timeout=self._timeout,
            follow_redirects=True,
        )

        if self._manual_cookie:
            self._client.headers["Cookie"] = self._manual_cookie

    @property
    def user_agent(self) -> str:
        """当前使用的 User-Agent，对外只读。"""

        return self._user_agent

    async def aclose(self) -> None:
        """关闭底层 httpx 客户端。"""

        await self._client.aclose()

    async def get_ttwid(self, *, force_refresh: bool = False) -> str:
        """访问抖音首页以获取响应 Cookie 中的 ttwid。

        Args:
            force_refresh: 是否强制重新获取（默认会缓存上一次结果）。

        Returns:
            ``ttwid`` 字符串。

        Raises:
            DouyinApiError: 抖音首页返回异常或响应中没有 ttwid。
        """

        if self._cached_ttwid and not force_refresh:
            return self._cached_ttwid

        url = "https://live.douyin.com/"
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise DouyinApiError(f"请求抖音首页失败: {exc}") from exc

        ttwid = resp.cookies.get("ttwid")
        if not ttwid:
            raise DouyinApiError("抖音首页响应未携带 ttwid Cookie")
        self._cached_ttwid = ttwid
        logger.debug("成功获取 ttwid")
        return ttwid

    async def get_room_id(self, live_id: str, *, ttwid: str | None = None) -> str:
        """根据 ``live_id`` (web_rid) 获取真正的 ``room_id``。

        Args:
            live_id: 直播间 ID（即 ``https://live.douyin.com/{live_id}`` 中的末段）。
            ttwid: 可选的 ttwid，若提供则带入 Cookie。

        Returns:
            真实 ``room_id`` 字符串。

        Raises:
            DouyinApiError: 页面请求失败或未能提取到 ``room_id``。
        """

        if not live_id:
            raise DouyinApiError("live_id 不能为空")

        url = f"https://live.douyin.com/{live_id}"
        headers: dict[str, str] = {}
        if ttwid:
            headers["Cookie"] = (
                f"ttwid={ttwid}; msToken={generate_ms_token()}; "
                "__ac_nonce=0123407cc00a9e438deb4"
            )

        try:
            resp = await self._client.get(url, headers=headers)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            raise DouyinApiError(f"请求直播间页面失败 (live_id={live_id}): {exc}") from exc

        html = resp.text
        for pattern in _ROOM_ID_PATTERNS:
            match = pattern.search(html)
            if match:
                room_id = match.group(1)
                logger.debug(f"提取 roomId 成功: live_id={live_id} -> room_id={room_id}")
                return room_id

        raise DouyinApiError(
            f"在直播间页面中未找到 roomId (live_id={live_id})；"
            "可能是直播间不存在或抖音前端结构变更"
        )


__all__ = ["DouyinApi", "DouyinApiError"]
