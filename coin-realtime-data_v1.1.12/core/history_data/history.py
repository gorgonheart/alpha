import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from core.coin_cap import coin_cap_update_to_recent, convert_coin_cap_csv
from core.handle import BmacHandle
from core.history_data.candle import init_history_candles
from core.history_data.funding import async_download_funding_rates
from core.history_data.utils import get_type_symbols
from core.utils.log_kit import divider, logger
from core.utils.time import convert_interval_to_timedelta, next_run_time, now_time, now_time_str


async def init_coin_cap(handle: BmacHandle, run_time: datetime):
    logger.info(f"开始初始化市值数据, 当前时间={now_time_str()}")
    t_start = time.perf_counter()
    convert_coin_cap_csv(handle.coin_cap_handle, run_time)

    await coin_cap_update_to_recent(handle, run_time)

    time_elapsed = (time.perf_counter() - t_start) / 60
    logger.ok(f"初始化市值数据成功, 当前时间={now_time_str()}, 耗时={time_elapsed:.2f}分钟")


def run_download_all_history(handle: BmacHandle):
    while True:
        # 计算历史数据 run_time
        run_time = next_run_time(handle.base_interval) - convert_interval_to_timedelta(handle.base_interval)
        divider(f"开始初始化历史数据, run_time={run_time}", with_timestamp=False)

        spot_symbols = asyncio.run(get_type_symbols(handle, "spot"))
        swap_symbols = asyncio.run(get_type_symbols(handle, "swap"))
        logger.info(f"现货交易对数量={len(spot_symbols)}, 永续合约交易对数量={len(swap_symbols)}")

        with ThreadPoolExecutor(max_workers=4) as executor:
            download_spot = executor.submit(init_history_candles, handle, "spot", run_time, spot_symbols)
            download_swap = executor.submit(init_history_candles, handle, "swap", run_time, swap_symbols)
            tasks = [download_spot, download_swap]

            if handle.fetch_funding_rate:
                download_funding = executor.submit(
                    asyncio.run, async_download_funding_rates(handle, run_time, swap_symbols)
                )
                tasks.append(download_funding)

            if handle.coin_cap_handle is not None:
                coin_cap = executor.submit(asyncio.run, init_coin_cap(handle, run_time))
                tasks.append(coin_cap)

            for download_task in as_completed(tasks):
                download_task.result()

        # 设置3个resample(1h)的容错，避免大参数下载资金费数据时间过长导致循环下载
        if now_time() - run_time < convert_interval_to_timedelta(handle.resample_interval) * 3:
            return run_time

        # 如果本期下载时间太长，导致结束时间距离 run_time 大于一个 resample(1h) 周期，则再下载一次
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
