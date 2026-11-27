"""把 stdlib logging 桥接进 log_kit，供主进程使用。

runtime_supervisor / alert / ts_manager 这些模块同时跑在主进程和 spawn 子进程里，
所以它们用标准库的 logging.getLogger(__name__)（库代码写法），由各进程入口决定 sink：

  - 子进程入口调 runtime_supervisor._configure_subprocess_logging（stdout + 独立文件）；
  - 主进程没有对应配置，root logger 无 handler → 这些模块在主进程产生的 INFO 日志
    （如动态订阅重启、supervisor 健康检查）全部被静默丢弃。

本模块补主进程这一侧：往 root logger 挂一个转发 handler，把传播上来的记录交给
log_kit 的 logger 输出（彩色控制台 + logs/*.log 落盘），格式与框架其余日志一致。
不改 log_kit（冻结接口），也不给 root 直接挂文件 handler（会与 log_kit 的
TimedRotatingFileHandler 在同一文件上重复轮转）。
"""
import logging

from core.utils.log_kit import logger as _logkit_logger


class LogKitBridgeHandler(logging.Handler):
    """root logger 上的转发 handler：stdlib 记录 → log_kit logger。"""

    def __init__(self, target=None):
        super().__init__(level=logging.INFO)
        self._target = target if target is not None else _logkit_logger
        self.setFormatter(logging.Formatter("%(name)s: %(message)s"))

    def emit(self, record):
        # log_kit 的 logger 自己也向 root 传播（propagate 默认 True），不跳过会无限递归；
        # 源 logger 已有 handler 说明它自己已输出过，桥接会造成重复
        if logging.getLogger(record.name).handlers:
            return
        try:
            self._target.log(record.levelno, self.format(record))
        except Exception:
            self.handleError(record)


def install_logkit_bridge(target=None) -> None:
    """主进程入口调用一次；幂等。root 级别放宽到 INFO（默认 WARNING 会在传播前就拦掉 INFO）。"""
    root = logging.getLogger()
    if any(isinstance(h, LogKitBridgeHandler) for h in root.handlers):
        return
    root.addHandler(LogKitBridgeHandler(target))
    if root.getEffectiveLevel() > logging.INFO:
        root.setLevel(logging.INFO)
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
