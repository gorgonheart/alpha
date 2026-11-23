"""令牌桶限速器（Token Bucket）。

用于约束对交易所 REST 接口的并发请求速率。

设计要点：
  - 计时统一使用 ``asyncio.get_event_loop().time()``（单调的事件循环时钟），
    不使用 ``time.time()`` / ``datetime.now()``，以便在 pytest-asyncio 等
    虚拟事件循环下行为可预测、可测试。
  - ``acquire`` 为协程：当桶内 token 不足时，按补充速率计算需等待的时长并
    ``await asyncio.sleep`` 到 token 足够，从而对调用方实现节流。
"""

import asyncio
import threading
import time
from collections import deque


class ProcessSlidingWindowLimiter:
    """进程级、跨线程共享的异步滑动窗口限速器。

    动机（FIX #4b）：OKX 的限速是「按 IP + 端点」计的。冷启动时 spot 与 swap 在
    ``ThreadPoolExecutor`` 的不同线程里各自跑独立的 asyncio loop，若每个
    ``OkxFetcher`` 各持一份 :class:`SlidingWindowLimiter`，两者并发会让同一 IP 上
    某端点的真实请求速率翻倍。本类用 ``threading.Lock`` + ``time.monotonic()`` 墙钟
    （而非 loop-time + asyncio.Lock）记账，使「多个 handle 共享同一窗口」在跨线程 /
    跨 loop 时仍严格成立。

    与 :class:`SlidingWindowLimiter` 的对外契约一致（``async def acquire(n=1)``），
    可在 :class:`OkxFetcher` 中直接替换 per-fetcher 限速器，无需改任何调用点。

    并发模型：
      - 共享状态（时间戳队列 + 一把 ``threading.Lock``）封装在 :class:`_SharedState`，
        由多个 handle 实例共享；线程安全靠这把锁保证。
      - ``acquire`` 在锁内只做「逐出过期戳 + 看是否有名额；有则记账返回，没有则算出
        需等待时长」，随后**释放锁**再 ``await asyncio.sleep(wait)``，醒来重试。
        绝不在持有 ``threading.Lock`` 时 ``await``，避免阻塞事件循环 / 串行化线程。
    """

    class _SharedState:
        """被多个 handle 共享的限速窗口状态（带一把 threading.Lock）。"""

        def __init__(self, limit: int, window_sec: float):
            self.limit = limit
            self.window_sec = window_sec
            self.lock = threading.Lock()
            self.stamps: deque[float] = deque()

    def __init__(self, state: "ProcessSlidingWindowLimiter._SharedState"):
        self._state = state

    def _try_reserve(self) -> float:
        """锁内尝试占用一个名额。

        Returns:
            0.0 表示已成功占用一个名额；>0 表示窗口已满，需 sleep 的秒数。
        """
        state = self._state
        with state.lock:
            now = time.monotonic()
            boundary = now - state.window_sec
            while state.stamps and state.stamps[0] <= boundary:
                state.stamps.popleft()
            if len(state.stamps) < state.limit:
                state.stamps.append(now)
                return 0.0
            wait = state.stamps[0] + state.window_sec - now
            return wait if wait > 0 else 0.0

    async def acquire(self, n: int = 1) -> None:
        """获取 ``n`` 个跨线程共享窗口名额；窗口已满时 await 直到足够名额滑出。"""
        for _ in range(n):
            while True:
                wait = self._try_reserve()
                if wait <= 0.0:
                    break
                await asyncio.sleep(wait)


# 进程级共享限速器单例：按端点（'recent' / 'history' / 'public'）各持一个窗口。
# OKX 限额：/market/candles 40 次/2s，/market/history-candles 20 次/2s。
#   - 'recent' 配置为 35/2s（而非贴上限 40）：留 ≈12% 余量，吸收 WS 回退扇出与
#     重试抖动，降低 candle 路径触发 429 的概率。
#   - 'history' 配置为 17/2s（而非贴上限 20）：本地按发出时间计数，OKX 按到达时间计数，
#     网络抖动会让请求在服务端扎堆；贴上限时 429 频发，每次 429 的退避又会拖住整轮下载。
#     模拟（服务端按到达时间严格 20/2s）：20/2s 有效吞吐约 990 bars/s；17/2s 配合历史下载
#     429 快速失败约 2500 bars/s，且 429 次数降到约 1/4。
#   - 'public' 为 instruments / funding 等公共端点共享的保守窗口（10/2s）：这些端点
#     此前直接走 async_retry_getter、完全绕过限速，与 candle 扇出叠加易触 429。
_OKX_LIMITER_SPECS = {
    "recent": (35, 2.0),
    "history": (17, 2.0),
    "public": (10, 2.0),
}
_okx_limiter_states: dict[str, ProcessSlidingWindowLimiter._SharedState] = {}
_okx_limiter_states_lock = threading.Lock()


def get_okx_shared_limiter(endpoint: str) -> ProcessSlidingWindowLimiter:
    """返回某端点的进程级共享限速器 handle（跨所有 OkxFetcher 实例共享窗口）。

    Args:
        endpoint: 'recent'（/market/candles）、'history'（/market/history-candles）
            或 'public'（instruments / funding 等公共端点共享窗口）。

    Returns:
        一个新的 :class:`ProcessSlidingWindowLimiter` handle，内部引用该端点进程级
        唯一的共享窗口状态——同一端点的不同 handle 共享同一窗口，从而把同一 IP 上
        的请求速率约束在端点限额之内。
    """
    if endpoint not in _OKX_LIMITER_SPECS:
        raise ValueError(f"unknown OKX limiter endpoint: {endpoint!r}")
    with _okx_limiter_states_lock:
        state = _okx_limiter_states.get(endpoint)
        if state is None:
            limit, window_sec = _OKX_LIMITER_SPECS[endpoint]
            state = ProcessSlidingWindowLimiter._SharedState(limit=limit, window_sec=window_sec)
            _okx_limiter_states[endpoint] = state
    return ProcessSlidingWindowLimiter(state)


class SlidingWindowLimiter:
    """异步滑动窗口限速器。

    严格保证：在任意长度为 ``window_sec`` 的滚动时间窗口内，最多放行 ``limit``
    个请求——包括「刚创建、桶为空」时的首个窗口（不存在 TokenBucket 那种初始装满
    导致首窗突发超额的问题）。

    设计要点：
      - 计时统一使用 ``asyncio.get_event_loop().time()``（单调的事件循环时钟），
        与 :class:`TokenBucket` 一致，便于在 pytest-asyncio 虚拟事件循环下测试。
      - 内部用一个时间戳双端队列记录最近 ``limit`` 次放行的完成时刻；新请求到来时
        先逐出已滑出窗口（早于 ``now - window_sec``）的时间戳，若窗口内仍占满 ``limit``
        个名额，则 ``await asyncio.sleep`` 到最早名额滑出为止。

    Args:
        limit: 每个滚动窗口内允许的最大请求数（即 N）。
        window_sec: 滚动窗口长度（秒），OKX 接口为 2.0。
    """

    def __init__(self, limit: int, window_sec: float):
        self.limit = limit
        self.window_sec = window_sec
        self._lock = asyncio.Lock()
        # 最近放行请求的完成时间戳（事件循环单调时钟），最多保留 limit 个。
        self._stamps: deque[float] = deque()

    def _now(self) -> float:
        """返回当前事件循环时间（单调）。"""
        return asyncio.get_event_loop().time()

    def _evict(self, now: float) -> None:
        """逐出已滑出当前窗口的时间戳。"""
        boundary = now - self.window_sec
        while self._stamps and self._stamps[0] <= boundary:
            self._stamps.popleft()

    async def acquire(self, n: int = 1) -> None:
        """获取 ``n`` 个窗口名额；窗口已满时 await 直到足够名额滑出。

        Args:
            n: 需要获取的名额数量，默认 1。
        """
        async with self._lock:
            for _ in range(n):
                while True:
                    now = self._now()
                    self._evict(now)
                    if len(self._stamps) < self.limit:
                        # 名额可用：记录本次放行时刻并占用一个名额。
                        self._stamps.append(now)
                        break
                    # 窗口已满：等到最早一个名额滑出窗口。
                    wait = self._stamps[0] + self.window_sec - now
                    if wait <= 0:
                        # 边界情形：理论上 _evict 已处理，这里兜底避免忙等。
                        wait = 0.0
                    await asyncio.sleep(wait)


class TokenBucket:
    """异步令牌桶限速器。

    Args:
        capacity: 桶容量（最大可累计的 token 数）。
        refill_per_sec: 每秒补充的 token 数量（补充速率）。
    """

    def __init__(self, capacity: int, refill_per_sec: float):
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        # 初始装满，允许突发到容量上限
        self._tokens: float = float(capacity)
        self._lock = asyncio.Lock()
        # 上次补充时间戳（事件循环单调时钟）
        self._last = asyncio.get_event_loop().time()

    def _now(self) -> float:
        """返回当前事件循环时间（单调）。"""
        return asyncio.get_event_loop().time()

    def _refill(self) -> None:
        """按经过的时间补充 token，封顶到容量。"""
        now = self._now()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_sec)
            self._last = now

    async def acquire(self, n: int = 1) -> None:
        """获取 ``n`` 个 token；不足时 await 直到补充足够。

        Args:
            n: 需要获取的 token 数量，默认 1。
        """
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return
                # 计算补足所需 token 对应的等待时长
                deficit = n - self._tokens
                wait = deficit / self.refill_per_sec
                await asyncio.sleep(wait)
