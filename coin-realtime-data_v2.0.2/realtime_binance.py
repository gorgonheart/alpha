# -*- coding: utf-8 -*-
"""Binance 实时数据采集入口。

显式绑定 config_binance，复用 core.app 的共享核心 run_realtime。
日志按入口脚本名自动落到 logs/realtime_binance.log（log_kit 用 sys.argv[0] 的 stem）。
"""
import config_binance
from core.app import run_realtime
from core.handle import create_binance_handle

if __name__ == "__main__":
    run_realtime(config_binance, create_binance_handle)
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
