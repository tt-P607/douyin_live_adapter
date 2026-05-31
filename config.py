"""douyin_live_adapter 插件配置。

配置抖音直播间 ID 以及连接参数。
"""

from __future__ import annotations

from typing import ClassVar

from src.app.plugin_system.base import BaseConfig, Field, SectionBase, config_section


class DouyinLiveAdapterConfig(BaseConfig):
    """抖音直播弹幕 Adapter 配置。"""

    config_name: ClassVar[str] = "config"
    config_description: ClassVar[str] = (
        "douyin_live_adapter 插件配置（直播间 ID + 签名参数）"
    )

    @config_section("plugin", title="插件总开关")
    class PluginSection(SectionBase):
        """插件级开关。

        关闭后插件本身仍然会被框架加载（adapter 也会被注册），但不会去拿
        ttwid / room_id / 建立 WebSocket 长连——相当于"占位但不工作"。
        想完全卸载请改 ``manifest.json`` 或移除插件目录。
        """

        enabled: bool = Field(
            default=True,
            description="是否启用本插件的长连功能；关闭后不会建立任何抖音 WS 长连",
        )

    @config_section("douyin", title="抖音直播配置")
    class DouyinSection(SectionBase):
        """抖音直播间配置。"""

        live_id: str = Field(
            default="",
            description="直播间 ID（即 https://live.douyin.com/{live_id} 中的 live_id 部分）",
        )
        stream_name: str = Field(
            default="",
            description=(
                "聊天流（直播间）的显示名；留空则用 ``\"抖音直播间 {room_id}\"`` 兜底。"
                "多平台同播时建议两个 adapter 填同一个值，stream 才能用统一名字。"
            ),
        )
        user_agent: str = Field(
            default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
            description="模拟浏览器的 User-Agent",
        )
        cookie: str = Field(
            default="",
            description="可选：手动 Cookie（高级用户用，留空即可由插件自动获取 ttwid）",
        )

    @config_section("connection", title="长连参数")
    class ConnectionSection(SectionBase):
        """WebSocket 长连接相关参数。"""

        heartbeat_interval: float = Field(
            default=5.0,
            description="WebSocket 心跳间隔（秒），抖音 Web 协议建议 5s",
        )
        auto_reconnect: bool = Field(
            default=True,
            description="长连断开后是否自动重连",
        )
        reconnect_initial_delay: float = Field(
            default=2.0,
            description="重连首次延迟（秒）；连续失败会指数退避",
        )
        reconnect_max_delay: float = Field(
            default=60.0,
            description="重连退避封顶（秒）",
        )
        request_timeout: float = Field(
            default=10.0,
            description="HTTP 请求超时（秒）",
        )

    @config_section("signature", title="签名配置")
    class SignatureSection(SectionBase):
        """JS 签名引擎相关配置。"""

        retry_max: int = Field(
            default=3,
            description="签名失败自动重试次数（抖音偶发返回空签名）",
        )

    plugin: PluginSection = Field(default_factory=PluginSection)
    douyin: DouyinSection = Field(default_factory=DouyinSection)
    connection: ConnectionSection = Field(default_factory=ConnectionSection)
    signature: SignatureSection = Field(default_factory=SignatureSection)


__all__ = ["DouyinLiveAdapterConfig"]
