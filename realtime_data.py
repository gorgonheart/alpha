# -*- coding: utf-8 -*-
"""
选币策略框架 | 邢不行 | 2024分享会
author: 邢不行
微信: xbx6660
"""
import asyncio
import sys
from datetime import datetime
from typing import Optional

import config
from core import app as _app
from core.handle import BmacHandle, create_bmac_handle_from_config
from core.pipeline.runtime_supervisor import RuntimeSupervisor

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _fetch_wake_time(run_time: datetime) -> datetime:
    """计算 REST 采集实际唤醒时间，逻辑 run_time 及闭合边界契约保持不变。"""
    return _app._fetch_wake_time(run_time, config)


def init_history(handle: BmacHandle, cfg=None):
    return _app.init_history(handle, config if cfg is None else cfg)


def update_all(
    handle: BmacHandle,
    run_time: datetime,
    cfg=None,
    supervisor: Optional[RuntimeSupervisor] = None,
):
    return _app.update_all(handle, run_time, config if cfg is None else cfg, supervisor=supervisor)


def main():
    _app.run_realtime(config, create_bmac_handle_from_config)


if __name__ == "__main__":
    main()
