# -*- coding: utf-8 -*-
"""OKX 实时数据采集入口。

显式绑定 config_okx，复用 core.app 的共享核心 run_realtime。
日志按入口脚本名自动落到 logs/realtime_okx.log（log_kit 用 sys.argv[0] 的 stem）。
"""
import config_okx
from core.app import run_realtime
from core.handle import create_okx_handle

if __name__ == "__main__":
    run_realtime(config_okx, create_okx_handle)
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
