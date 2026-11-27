import asyncio
import random

import aiohttp

from core.utils.log_kit import logger

# 429 专用退避基础值（秒）。明显长于非 429 的固定 1/2/4/8/16s schedule，
# 配合 jitter 让 spot/swap 等并行循环不锁步重试，给限频窗口留出冷却时间。
_RATE_LIMIT_BASE_SLEEP = 5


def is_rate_limited(e: Exception) -> bool:
    """Duck-typing 判断异常是否为 429 / Too Many Requests，避免 import 交易所异常类。"""
    if getattr(e, 'status_code', None) == 429:
        return True
    if getattr(e, 'code', None) in (50011, '50011'):
        return True
    if 'Too Many Requests' in str(e):
        return True
    return False


async def async_retry_getter(func, _max_times=5, _sleep_seconds=1, **kwargs):
    _rate_limit_sleep = _RATE_LIMIT_BASE_SLEEP
    while True:
        try:
            return await func(**kwargs)
        except Exception as e:
            if _max_times == 0:
                logger.error('错误 %s, 剩余 0 次重试机会, 出错函数 %s', repr(e), repr(func))
                raise e

            if is_rate_limited(e):
                # 429：更长的基础退避 + jitter（不影响非 429 路径的固定 schedule）。
                await asyncio.sleep(_rate_limit_sleep + random.uniform(0, _rate_limit_sleep))
                _rate_limit_sleep *= 2
            else:
                # 非 429：保持原始 1/2/4/8/16s 固定退避，无 jitter（Binance 路径零回归）。
                await asyncio.sleep(_sleep_seconds)
                _sleep_seconds *= 2
            _max_times -= 1


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session



#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
