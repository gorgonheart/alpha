"""
告警分级语义（团队所有 alert 调用必须遵循）：

warn:
  - 触发条件: 系统能自愈的临时异常
  - 例子: WS reconnect 一次成功 / queue 占用 50-80% / 单 symbol REST 拉取失败 1 次
  - 推送策略: 仅写日志 + SLA 看板计数，不推送企微
  - 去抖: 同一 key 每分钟最多 1 条

error:
  - 触发条件: 影响数据正确性但有兜底
  - 例子: barrier timeout 跳过某 symbol / reconcile 偏差 > 0.1% / ws 某分片反复重连
  - 推送策略: 连续 3 次相同 key 才推送企微
  - 去抖: 同一 key 每 5 分钟最多 1 条

critical:
  - 触发条件: 系统持续异常或潜在数据损坏
  - 例子: WS 全断 + REST 也失败 / queue 满反压触发 / writer 进程死亡 / pickle 加载失败
  - 推送策略: 立即推送企微 (维护窗口内 +1 档去抖)
  - 去抖: 同一 key 每 1 分钟最多 1 条

实现说明：
  - 类 `Alert` 暴露 warn(key, msg)、error(key, msg)、critical(key, msg)。
  - 推送阈值：warn=NEVER（仅日志）、error=连续 3 次同 key、critical=1 次即推。
  - 各级别独立去抖窗口：warn=60s、error=300s、critical=60s。可通过构造参数
    `dedupe_windows` 配置。推送后，同一 (level, key) 在该级别窗口内被抑制。
  - 不同 key 计数器相互独立。
  - 推送（或被抑制）会重置该 key 的连续计数。
  - webhook_url 经校验：非 None 时必须以 http:// 或 https:// 开头。
  - 后端：core.utils.dingding.send_wechat_work_msg(msg, webhook_url)。
"""
from __future__ import annotations

import logging
import time
from typing import Dict, Optional

from core.utils.dingding import send_wechat_work_msg


logger = logging.getLogger(__name__)


# 各级别的推送阈值：同 key 连续出现多少次才推送。负值 = 永不推送（仅日志）。
_THRESHOLD: Dict[str, int] = {
    "warn": -1,      # log only
    "error": 3,
    "critical": 1,
}

# 各级别的去抖窗口（秒）。对某 (level, key) 推送之后，同一 (level, key) 的后续
# 推送在该秒数内被抑制。
_DEFAULT_DEDUPE_WINDOWS: Dict[str, int] = {
    "warn": 60,      # warn 永不推送，这里只影响计数器重置
    "error": 300,
    "critical": 60,
}


class Alert:
    """三级告警：连续 N 次阈值 + 按 key 去抖。

    Args:
        webhook_url: 企业微信 webhook。None 时禁用实际推送（调用仍会走后端，
            后端在 url 为空时自身就是 no-op）。
        dedupe_window_sec: 推送之后，同 key 的后续推送在该秒数内被抑制。
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        dedupe_window_sec: Optional[int] = None,
        dedupe_windows: Optional[Dict[str, int]] = None,
    ) -> None:
        # 校验 webhook URL：空串或非 http(s) scheme 一律拒绝，避免错配的
        # config.json 把消息误发到 paths / javascript: / ftp:// 之类。
        if webhook_url is not None:
            if not isinstance(webhook_url, str) or not webhook_url.startswith(
                ("http://", "https://")
            ):
                raise ValueError(
                    f"webhook_url must start with http:// or https://, got {webhook_url!r}"
                )
        self._webhook_url: Optional[str] = webhook_url

        # 解析各级别的去抖窗口。优先级：
        #   1. 显式传入的 `dedupe_windows` dict
        #   2. 旧的 `dedupe_window_sec` int（向后兼容，统一应用到所有级别）
        #   3. _DEFAULT_DEDUPE_WINDOWS
        if dedupe_windows is not None:
            self._dedupe_windows: Dict[str, int] = {
                level: int(dedupe_windows.get(level, _DEFAULT_DEDUPE_WINDOWS[level]))
                for level in _DEFAULT_DEDUPE_WINDOWS
            }
        elif dedupe_window_sec is not None:
            self._dedupe_windows = {
                level: int(dedupe_window_sec) for level in _DEFAULT_DEDUPE_WINDOWS
            }
        else:
            self._dedupe_windows = dict(_DEFAULT_DEDUPE_WINDOWS)

        # 每个 (level, key) 自上次推送/窗口重置后的连续计数。
        self._counts: Dict[tuple[str, str], int] = {}
        # 每个 (level, key) 上次推送的 monotonic 时间戳（用于去抖窗口）。
        self._last_push_ts: Dict[tuple[str, str], float] = {}

    # ---- 公共 API ----

    def warn(self, key: str, msg: str) -> None:
        self._handle("warn", key, msg)

    def error(self, key: str, msg: str) -> None:
        self._handle("error", key, msg)

    def critical(self, key: str, msg: str) -> None:
        self._handle("critical", key, msg)

    # ---- 内部 ----

    def _handle(self, level: str, key: str, msg: str) -> None:
        threshold = _THRESHOLD[level]
        window_sec = self._dedupe_windows[level]
        ck = (level, key)

        # 总是写日志，让 SLA 看板和本地观察者能看到事件。
        log_fn = getattr(logger, level if level != "critical" else "critical")
        log_fn("[alert.%s] key=%s msg=%s", level, key, msg)

        # warn 按规范仅日志 —— 无论计数多少都不推送。
        if threshold < 0:
            return

        # 各级别去抖窗口：若该 (level, key) 刚推过，无论计数多少都抑制，并把计数清零。
        now = time.monotonic()
        last_ts = self._last_push_ts.get(ck)
        if last_ts is not None and (now - last_ts) < window_sec:
            self._counts[ck] = 0
            return

        # 自增连续计数，判断是否推送。
        self._counts[ck] = self._counts.get(ck, 0) + 1
        if self._counts[ck] >= threshold:
            self._push(msg)
            # 推送后重置该 key 的计数，并开启去抖窗口。
            self._counts[ck] = 0
            self._last_push_ts[ck] = now

    def _push(self, msg: str) -> None:
        """走后端发送。测试里后端会被 monkeypatch。"""
        try:
            send_wechat_work_msg(msg, self._webhook_url)
        except Exception:  # pragma: no cover - defensive, backend already swallows
            logger.exception("[alert] send_wechat_work_msg raised unexpectedly")
