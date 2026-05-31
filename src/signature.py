"""douyin_live_adapter 签名引擎。

封装 ``py-mini-racer``（嵌入式 V8）执行抖音 Web 版的 JS 签名算法：

- :meth:`DouyinSigner.gen_signature` 计算 WebSocket URL 的 ``signature`` 参数。
- :meth:`DouyinSigner.gen_a_bogus` 计算 HTTP 接口的 ``a_bogus`` 参数（备用）。

JS 资源（``sign.js`` / ``a_bogus.js``）放在插件的 ``assets/`` 目录下；版本随上游
``DouyinLiveWebFetcher`` 同步更新。
"""

from __future__ import annotations

import hashlib
import random
import string
import urllib.parse
from pathlib import Path
from typing import Any

from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("douyin_live_adapter.signature")


# 抖音 WSS URL 中参与 signature 计算的 13 个 query key，顺序固定。
# 与 ``DouyinLiveWebFetcher.liveMan.generateSignature`` 保持一致。
_SIGNATURE_PARAM_KEYS: tuple[str, ...] = (
    "live_id",
    "aid",
    "version_code",
    "webcast_sdk_version",
    "room_id",
    "sub_room_id",
    "sub_channel_id",
    "did_rule",
    "user_unique_id",
    "device_platform",
    "device_type",
    "ac",
    "identity",
)


def generate_ms_token(length: int = 107) -> str:
    """产生抖音 Web 端常用的 ``msToken`` 字段（随机字符串）。

    Args:
        length: 字符长度，抖音常用 107。
    """

    base_str = string.ascii_letters + string.digits + "-_"
    return "".join(random.choices(base_str, k=length))


class DouyinSigner:
    """抖音签名生成器，封装 ``py-mini-racer``。

    生命周期：
    - 构造时只记录 JS 资源路径，不立即编译（避免冷启开销）。
    - 首次调用 ``gen_signature`` / ``gen_a_bogus`` 时懒加载 JS。
    - 插件卸载时调 :meth:`close` 显式释放（V8 上下文随对象 GC，但显式释放更稳）。
    """

    def __init__(self, *, assets_dir: Path) -> None:
        """初始化签名器。

        Args:
            assets_dir: ``sign.js`` / ``a_bogus.js`` 所在目录。
        """

        self._assets_dir = assets_dir
        # 不在 import 顶层引用 ``py_mini_racer``，避免框架在 manifest 声明的
        # 依赖被自动安装之前就因为 import 错误而失败。MiniRacer 的实例与类
        # 都在 :meth:`_ensure_ctx` 中按需创建；类型注解使用 ``Any``。
        self._ctx: Any | None = None
        self._sign_loaded = False
        self._a_bogus_loaded = False

    def _ensure_ctx(self):  # noqa: ANN202 - MiniRacer 类延迟导入
        """惰性创建 V8 上下文。"""

        if self._ctx is None:
            try:
                from py_mini_racer import MiniRacer
            except ImportError as exc:
                raise RuntimeError(
                    "未安装 py-mini-racer；请检查 manifest.json 的 python_dependencies"
                ) from exc
            self._ctx = MiniRacer()
        return self._ctx

    def _ensure_sign_loaded(self):  # noqa: ANN202
        """懒加载 ``sign.js``。"""

        ctx = self._ensure_ctx()
        if self._sign_loaded:
            return ctx

        sign_js = self._assets_dir / "sign.js"
        if not sign_js.exists():
            raise RuntimeError(f"sign.js 未找到: {sign_js}")
        try:
            ctx.eval(sign_js.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error(f"加载 sign.js 失败: {exc}")
            raise RuntimeError(f"加载 sign.js 失败: {exc}") from exc
        self._sign_loaded = True
        return ctx

    def _ensure_a_bogus_loaded(self):  # noqa: ANN202
        """懒加载 ``a_bogus.js``。"""

        ctx = self._ensure_ctx()
        if self._a_bogus_loaded:
            return ctx

        ab_js = self._assets_dir / "a_bogus.js"
        if not ab_js.exists():
            raise RuntimeError(f"a_bogus.js 未找到: {ab_js}")
        try:
            ctx.eval(ab_js.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error(f"加载 a_bogus.js 失败: {exc}")
            raise RuntimeError(f"加载 a_bogus.js 失败: {exc}") from exc
        self._a_bogus_loaded = True
        return ctx

    @staticmethod
    def _compute_md5_param(wss_url: str) -> str:
        """从 WSS URL 中提取 13 个字段 → 拼接 → MD5。

        提取顺序、拼接格式与抖音前端 `byted_acrawler.frontierSign` 的预期一致。
        """

        query = urllib.parse.urlparse(wss_url).query
        # 用 ``parse_qsl`` 而不是 ``parse_qs`` 可以保持原始顺序，但对于本场景
        # 我们只需要按 key 取值，因此 ``parse_qs`` 取第一个就够了。
        parsed = urllib.parse.parse_qs(query, keep_blank_values=True)
        wss_maps = {k: (v[0] if v else "") for k, v in parsed.items()}
        tpl_params = [f"{k}={wss_maps.get(k, '')}" for k in _SIGNATURE_PARAM_KEYS]
        param_str = ",".join(tpl_params)
        return hashlib.md5(param_str.encode("utf-8")).hexdigest()

    def gen_signature(self, wss_url: str, *, retry_max: int = 3) -> str | None:
        """生成 WebSocket URL 所需的 ``signature`` 字段。

        抖音的 ``get_sign`` 偶发返回空字符串（原项目注释 ``成功率不是100%``），
        本方法在内部按 ``retry_max`` 次重试。

        Args:
            wss_url: 含必要 query 的 WSS URL（不带 ``signature`` 参数）。
            retry_max: 最大尝试次数（含首次）。

        Returns:
            32 位 16 进制小写签名串；连续重试均失败时返回 ``None``。
        """

        ctx = self._ensure_sign_loaded()
        md5_param = self._compute_md5_param(wss_url)
        attempts = max(1, retry_max)

        for attempt in range(1, attempts + 1):
            try:
                signature = ctx.call("get_sign", md5_param)
            except Exception as exc:
                logger.warning(f"调用 get_sign 异常 (第 {attempt}/{attempts} 次): {exc}")
                continue

            if signature:
                return str(signature)
            logger.debug(f"get_sign 返回空字符串 (第 {attempt}/{attempts} 次)")

        logger.warning(f"生成 signature 连续 {attempts} 次失败")
        return None

    def gen_a_bogus(self, url_params: dict[str, str], user_agent: str) -> str | None:
        """生成 HTTP 接口请求所需的 ``a_bogus`` 字段。

        Args:
            url_params: 待签名的 query 参数字典。
            user_agent: 当前使用的 User-Agent（必须与实际请求一致）。
        """

        ctx = self._ensure_a_bogus_loaded()
        try:
            query_str = urllib.parse.urlencode(url_params)
            a_bogus = ctx.call("get_ab", query_str, user_agent)
            return str(a_bogus) if a_bogus else None
        except Exception as exc:
            logger.warning(f"生成 a_bogus 失败: {exc}")
            return None

    def close(self) -> None:
        """显式释放 V8 上下文。

        ``py-mini-racer`` 的 ``MiniRacer`` 依赖 V8 native 资源；虽然 GC 时也会
        清理，但插件卸载时显式置 ``None`` 可以加快释放。
        """

        self._ctx = None
        self._sign_loaded = False
        self._a_bogus_loaded = False


__all__ = ["DouyinSigner", "generate_ms_token"]
