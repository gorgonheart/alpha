"""可选历史资金费回补；额度、轮转和冷却仅保留在父进程内存中。"""

import asyncio
import math
import os
import shutil
import time
from bisect import bisect_right
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd

from core.binance_fetcher_async import BinanceFetcher
from core.binance_market_async import BinanceAPIException
from core.funding_hours import normalize_funding_hours
from core.utils.log_kit import logger

BACKFILL_HOURS = 24
REQUEST_LIMIT = 450
WINDOW_SECONDS = 300
CONCURRENCY = 4
NETWORK_SECONDS = 8

_request_times = deque()
_last_symbol = None
_cooldown_until = 0.
_history_stopped = False
_round_hour = None
_hour_symbols = set()
_completed_symbols = set()
_next_run_time = None


def defer_funding_backfill():
    """初始化成功后隔离可选回补，不等待、不清除进程内已有状态。"""
    global _cooldown_until
    _cooldown_until = max(_cooldown_until, time.monotonic() + WINDOW_SECONDS)


def settled_funding_hours(frame):
    """Sum distinct settlements observed since the start of each UTC hour."""
    if frame.empty:
        return frame
    events = frame.drop_duplicates(['symbol', 'candle_begin_time'], keep='last').copy()
    events['candle_begin_time'] = events['candle_begin_time'].dt.floor('h')
    return events.groupby(['symbol', 'candle_begin_time'], as_index=False)['funding_rate'].sum()


def save_funding_hours(manager, symbol, run_time, incoming, depth):
    """Keep one canonical value per UTC hour and publish the funding PKL atomically."""
    stored = manager.read_candle(symbol) if manager.has_symbol(symbol) else None
    if stored is not None:
        previous = normalize_funding_hours(stored)
        if not previous.empty:
            incoming = pd.concat([previous, incoming], ignore_index=True)
    merged = incoming.dropna(subset=['candle_begin_time']).drop_duplicates('candle_begin_time', keep='last')
    merged = merged.sort_values('candle_begin_time')
    if depth is not None:
        merged = merged.tail(depth)
    merged = merged.reset_index(drop=True)
    if stored is not None and stored.equals(merged) and manager.check_ready(symbol, run_time):
        return
    destination = Path(manager.format_data_file_path(symbol))
    temp_path = None
    try:
        with NamedTemporaryFile(dir=destination.parent, suffix='.tmp', delete=False) as output:
            temp_path = Path(output.name)
            merged.to_pickle(output)
        if stored is not None:
            shutil.copymode(destination, temp_path)
        os.replace(temp_path, destination)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
    manager.save_ready(symbol, run_time)


def mark_funding_backfill_completed(round_hour, symbol):
    """非空历史仅在保存成功后确认；旧小时或重复确认不生效。"""
    if round_hour != _round_hour or symbol not in _hour_symbols or symbol in _completed_symbols:
        return False
    _completed_symbols.add(symbol)
    return True


def recent_funding_window(run_time):
    """Inclusive raw millisecond bounds, shared by startup and periodic correction."""
    # Include newly settled events in the current hour. Fetch the prefix of the
    # leading hour too, otherwise replacing that hour could drop an earlier event.
    end_ms = min(int(time.time() * 1000), int(run_time.timestamp() * 1000) - 1)
    hour_ms = 3_600_000
    start_ms = (end_ms // hour_ms - BACKFILL_HOURS) * hour_ms
    return start_ms, end_ms


async def backfill_recent_funding_rates(handle, session, run_time, symbols, *, deadline):
    """单轮串行调用；只下载，所有 worker 回收后才由元数据入口统一落盘。"""
    started = time.monotonic()
    frames = {}
    start_ms, end_ms = recent_funding_window(run_time)
    round_hour = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    symbols = set(symbols)
    if round_hour == _round_hour:
        # 本小时新增币种等下小时；下架币种不再占据待补集合。
        _hour_symbols.intersection_update(symbols)
        _completed_symbols.intersection_update(_hour_symbols)
    stats: dict = dict(requests=0, success=0, empty=0, failed=0, elapsed=0., reason='complete',
                       round_hour=round_hour, target=0, completed=0, remaining=0)
    try:
        if not handle.fetch_funding_rate or _history_stopped or started < _cooldown_until:
            stats['reason'] = 'disabled' if not handle.fetch_funding_rate or _history_stopped else 'cooldown'
            return frames, stats
        if _round_hour is not None and round_hour < _round_hour:
            stats['reason'] = 'stale_hour'
            stats['round_hour'] = _round_hour
            return frames, stats
        if round_hour == _round_hour:
            symbols = _hour_symbols - _completed_symbols
            if not symbols:
                stats['reason'] = 'hour_complete'
                return frames, stats
        if _next_run_time is not None and run_time < _next_run_time:
            stats['reason'] = 'interval'
            return frames, stats

        symbols = sorted(symbols)
        position = bisect_right(symbols, _last_symbol) if _last_symbol else 0
        queue = iter(symbols[position:] + symbols[:position])
        fetcher = BinanceFetcher(handle.binance_swap.api_type, session, handle.proxy)
        stop = asyncio.Event()

        async def worker():
            global _last_symbol, _cooldown_until, _history_stopped
            global _round_hour, _hour_symbols, _completed_symbols, _next_run_time
            while not stop.is_set():
                now = time.monotonic()
                if now >= deadline:
                    stats['reason'] = 'deadline'
                    return
                if stats['requests'] >= REQUEST_LIMIT:
                    stats['reason'] = 'round_limit'
                    return
                while _request_times and _request_times[0] <= now - WINDOW_SECONDS:
                    _request_times.popleft()
                if len(_request_times) >= REQUEST_LIMIT:
                    wait = _request_times[0] + WINDOW_SECONDS - now
                    if now + wait >= deadline:
                        stats['reason'] = 'rolling_limit'
                        return
                    await asyncio.sleep(wait)
                    continue
                # 从额度检查到预留/推进游标之间无 await，失败和取消均不退还。
                symbol = next(queue, None)
                if symbol is None:
                    return
                if _round_hour != round_hour:
                    # 仅有实际执行机会才冻结新小时名单，跳过不推进状态。
                    _round_hour = round_hour
                    _hour_symbols = set(symbols)
                    _completed_symbols = set()
                _next_run_time = run_time + pd.Timedelta(minutes=10)
                _request_times.append(now)
                _last_symbol = symbol
                stats['requests'] += 1
                try:
                    frame = await fetcher.get_hist_funding_rate(
                        symbol, startTime=start_ms, endTime=end_ms, limit=1000,
                        _max_times=0,
                    )
                    if len(frame) >= 1000:
                        raise ValueError('Truncated historical funding window')
                    frame = settled_funding_hours(frame)
                except BinanceAPIException as exc:
                    if exc.status_code in (403, 429, 418):
                        try:
                            delay = float(exc.response.headers.get('Retry-After'))
                        except (ValueError, TypeError, AttributeError):
                            delay = 0.
                        if not math.isfinite(delay) or delay <= 0:
                            delay = WINDOW_SECONDS if exc.status_code == 429 else 0.
                        if exc.status_code == 403:
                            delay = max(600., delay)
                        if delay:
                            _cooldown_until = max(_cooldown_until, time.monotonic() + delay)
                        else:
                            _history_stopped = True
                            logger.warning('418 无有效解封期限，停用本进程可选历史资金费回补')
                        if not stop.is_set():
                            stats['reason'] = f'http_{exc.status_code}'
                        stop.set()
                except Exception:
                    # 可选历史下载/校验失败不影响必需的实时更新。
                    pass
                else:
                    if frame.empty:
                        stats['empty'] += 1
                        mark_funding_backfill_completed(round_hour, symbol)
                    else:
                        frames[symbol] = frame
                        stats['success'] += 1

        workers = [asyncio.create_task(worker()) for _ in range(min(CONCURRENCY, len(symbols)))]
        stop_task = asyncio.create_task(stop.wait())
        tasks: list[asyncio.Task] = [*workers, stop_task]
        pending = set(workers)
        try:
            while pending:
                if stop.is_set():
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    stats['reason'] = 'deadline'
                    break
                done, _ = await asyncio.wait(pending | {stop_task}, timeout=remaining,
                                             return_when=asyncio.FIRST_COMPLETED)
                if stop.is_set():
                    break
                if not done:
                    stats['reason'] = 'deadline'
                    break
                pending.difference_update(done)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return frames, stats
    finally:
        stats['failed'] = stats['requests'] - stats['success'] - stats['empty']
        stats['elapsed'] = time.monotonic() - started
        if stats['round_hour'] == _round_hour:
            stats.update(target=len(_hour_symbols), completed=len(_completed_symbols),
                         remaining=len(_hour_symbols - _completed_symbols))
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
