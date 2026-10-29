"""
这个是我用于日志、输出、调试的日志工具，直接根据这个使用即可，不要去做调整

**使用方式如下**
```python
# script.py
from utils.log_kit import logger, divider

# 输出日志信息
logger.debug("调试信息，没有标记和颜色，等同于print")
logger.info("提示信息，蓝色的，可以记录一些中间结果")
logger.ok("完成提示，绿色的，通常表示成功和完成")
logger.warning("警告信息，黄色的，通常表示警告")
logger.error("错误信息，红色的，通常是报错的相关提示")
logger.critical("重要提示，深红色。通常是非常关键的信息")
divider('这个是我做的分割线的功能')
divider('点点是可以换的', sep='*')
divider('文字是居中的哦，英文和中文我尽量适配了。。。', sep='-')
```
"""

import logging
import multiprocessing
import os
import re
import sys
import time
import unicodedata
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from colorama import Fore, Style, init

from core.utils.path_kit import get_file_path, get_folder_path

init(autoreset=True)

current_script = Path(sys.argv[0]).stem

# ====================================================================================================
# ** 添加ok的日志级别 **
# 给默认的logging模块，添加一个用于表达成功的级别
# 我给他取名叫ok，以后logger.ok就能输出一个表示成功的信息
# ====================================================================================================
OK_LEVEL = 25
logging.addLevelName(OK_LEVEL, "OK")


def ok(self, message, *args, **kwargs):
    if self.isEnabledFor(OK_LEVEL):
        self._log(OK_LEVEL, message, args, **kwargs)


logging.Logger.ok = ok


# ====================================================================================================
# ** 辅助函数 **
# - get_display_width(): 获取文本的显示宽度，中文字符算作1.685个宽度单位，以尽量保持显示居中
# ====================================================================================================
def get_display_width(text: str) -> int:
    """
    获取文本的显示宽度，中文字符算作1.685个宽度单位，以尽量保持显示居中
    :param text: 输入的文本
    :return: 文本的显示宽度
    """
    width = 0
    for char in text:
        if unicodedata.east_asian_width(char) in ('F', 'W', 'A'):
            width += 1.685
        else:
            width += 1
    return int(width)


# ====================================================================================================
# ** 西蒙斯日志工具 **
# - SimonsFormatter(): 自定义日志格式
# - SimonsConsoleHandler(): 自定义控制台输出
# - SimonsLogger(): 日志工具
# 然而你不懂就不要改了，也没什么好改的，也欢迎增强分享
# ====================================================================================================
# ** 多进程日志支持 **
# - _is_worker_process(): 判断是不是 multiprocessing 派生出来的子进程
# - SafeTimedRotatingFileHandler: 轮转失败不炸、不刷屏的按天日志
# - _worker_log_path() / _cleanup_worker_logs(): 子进程独立日志的路径与清理
#
# 背景：ProcessPoolExecutor 的每个 worker 都会 import 本模块，如果它们和主进程一起打开
# 同一个 logs/realtime_data.log，在 Windows 上到午夜轮转时 os.rename 会因为其它进程
# 仍持有句柄而抛 PermissionError: [WinError 32]，标准库随后每写一条日志就重试一次，
# 于是刷出大量 "--- Logging error ---" 的堆栈。
# 处理方式：只有主进程负责写主日志并轮转，子进程写自己当天的 worker 日志（append，不做 rename）。
# ====================================================================================================
WORKER_LOG_KEEP_DAYS = 7  # 子进程日志保留天数


def _is_worker_process() -> bool:
    """判断当前是否 multiprocessing 子进程（spawn / fork 都适用）"""
    try:
        return multiprocessing.current_process().name != 'MainProcess'
    except Exception:
        return False


def _worker_log_path(name: str) -> str:
    """子进程日志按天命名，多进程之间的 append 写是安全的，不需要 rename 轮转"""
    return get_file_path('logs', f"{name.lower()}-worker-{datetime.now():%Y-%m-%d}.log")


def _cleanup_worker_logs(name: str, keep_days: int = WORKER_LOG_KEEP_DAYS) -> None:
    """清理过期的子进程日志，只在主进程启动时执行"""
    log_dir = get_folder_path('logs')
    pattern = re.compile(rf"^{re.escape(name.lower())}-worker-\d{{4}}-\d{{2}}-\d{{2}}\.log$")
    deadline = time.time() - keep_days * 86400
    try:
        file_names = os.listdir(log_dir)
    except OSError:
        return
    for file_name in file_names:
        if not pattern.match(file_name):
            continue
        full_path = os.path.join(log_dir, file_name)
        try:
            if os.path.getmtime(full_path) < deadline:
                os.remove(full_path)
        except OSError:
            # 正在被其它进程写入，忽略，下次启动再清
            pass


def _stdout_is_file(file_path: str) -> bool:
    """
    判断标准输出是否已经指向 file_path 这个文件
    （`python xxx.py > logs/xxx.log` 启动时，stdout 就是日志文件本身）

    Windows 下 sys.stdout.name 只会是 '<stdout>'，拿不到真实文件名，
    所以用 fstat 比较文件标识（Linux 上同样适用）。
    """
    try:
        fd_stat = os.fstat(sys.stdout.fileno())
        file_stat = os.stat(file_path)
    except (AttributeError, OSError, ValueError):
        return False
    if fd_stat.st_dev and fd_stat.st_ino:
        return (fd_stat.st_dev, fd_stat.st_ino) == (file_stat.st_dev, file_stat.st_ino)

    # 文件标识不可用时退回路径比较
    name = getattr(sys.stdout, 'name', None)
    if isinstance(name, str) and os.path.isfile(name):
        return os.path.abspath(name) == os.path.abspath(file_path)
    return False


class SafeTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    在标准库 TimedRotatingFileHandler 之上做了两件事：
    1. 轮转失败（Windows 上文件被其它进程或编辑器占用 → WinError 32）时不让异常冒到 logging，
       否则每写一条日志都会打印一段 "--- Logging error ---" 堆栈；
    2. 失败后把下次轮转推迟 1 小时，避免每秒都重试一次。
    """

    retry_interval = 3600  # 轮转失败后的重试间隔（秒）

    def doRollover(self):
        try:
            super().doRollover()
        except Exception as e:  # noqa: BLE001 轮转失败绝不能影响业务
            self._on_rollover_failed(e)

    def _on_rollover_failed(self, exc):
        # 标准库在 rename 之前就把 stream 关了，这里保证还能继续写当前文件
        if self.stream is None:
            try:
                self.stream = self._open()
            except OSError:
                pass
        # 推迟下一次轮转检查，避免每条日志都重试造成刷屏
        self.rolloverAt = time.time() + self.retry_interval

        now = time.time()
        if now - getattr(self, '_last_warn_ts', 0) > self.retry_interval:
            self._last_warn_ts = now
            try:
                print(f"[log_kit] 日志轮转失败({exc})，继续写入 {self.baseFilename}", flush=True)
            except Exception:
                pass

    def handleError(self, record):
        """兜底：日志本身出错时静默，不要把 traceback 打到业务输出里"""


class SimonsFormatter(logging.Formatter):
    FORMATS = {
        logging.DEBUG: ('', ''),
        logging.INFO: (Fore.BLUE, "🌀 "),
        logging.WARNING: (Fore.YELLOW, "🔔 "),
        logging.ERROR: (Fore.RED, "❌ "),
        logging.CRITICAL: (Fore.RED + Style.BRIGHT, "⭕ "),
        OK_LEVEL: (Fore.GREEN, "✅ "),
    }

    def format(self, record):
        color, prefix = self.FORMATS.get(record.levelno, (Fore.WHITE, ""))
        record.msg = f"{color}{prefix}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


class SimonsConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        if record.levelno == logging.DEBUG:
            print(record.msg, flush=True)
        elif record.levelno == OK_LEVEL:
            super().emit(record)
            print()
        else:
            super().emit(record)


# noinspection PyProtectedMember
class SimonsLogger:
    _instance = dict()

    def __new__(cls, name='QuantClassPro'):
        if cls._instance.get(name) is None:
            cls._instance[name] = super(SimonsLogger, cls).__new__(cls)
            cls._instance[name]._initialize_logger(name)
        return cls._instance[name]

    def _initialize_logger(self, name):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # 如果有handlers，就清理掉（顺手关闭，避免文件句柄泄漏导致后续轮转失败）
        if self.logger.hasHandlers():
            for handler in list(self.logger.handlers):
                self.logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass

        # 添加文件输出
        file_handler = self._create_file_handler(name)
        if file_handler is not None:
            file_fmt = "%(asctime)s [%(levelname)s] [%(processName)s] - %(message)s" if _is_worker_process() \
                else "%(asctime)s [%(levelname)s] - %(message)s"
            file_handler.setFormatter(logging.Formatter(file_fmt))
            self.logger.addHandler(file_handler)

        # 添加命令行输出
        console_handler = SimonsConsoleHandler(sys.stdout)
        console_handler.setFormatter(SimonsFormatter("%(message)s"))
        self.logger.addHandler(console_handler)

    @staticmethod
    def _create_file_handler(name):
        """
        主进程：写 logs/<name>.log，按天轮转（轮转做了失败兜底）
        子进程：写 logs/<name>-worker-<日期>.log，只追加不 rename，因此可以多进程共享

        特殊情况：标准输出被重定向到日志文件本身（`python xxx.py > logs/xxx.log`）时，
        同一个文件会被 stdout 和 handler 各写一遍，而且 rename 永远失败，所以直接跳过文件 handler。
        """
        log_path = _worker_log_path(name) if _is_worker_process() else get_file_path('logs', f"{name.lower()}.log")

        if _stdout_is_file(log_path):
            print(f"[log_kit] 标准输出已重定向到 {log_path}，跳过文件 handler"
                  f"（日志仍会通过 stdout 写入该文件；建议重定向到另一个文件以便正常轮转）", flush=True)
            return None

        if _is_worker_process():
            return logging.FileHandler(log_path, mode='a', encoding='utf-8', delay=True)

        # 主进程启动时清理过期的 worker 日志，避免 logs 目录越积越多
        _cleanup_worker_logs(name)
        return SafeTimedRotatingFileHandler(
            log_path, when='midnight', encoding='utf-8', interval=1, backupCount=7, delay=True
        )


# ====================================================================================================
# ** 功能函数 **
# - get_logger(): 获取日志对象，可以指定名称来区分日志
# - divider(): 画一个带时间戳的横线
# - logger: 默认的日志对象，独立跑一个脚本的话，可以直接用这个
# ====================================================================================================
def get_logger(name=None):
    if name is None:
        name = current_script
    return SimonsLogger(name).logger


def divider(name='', sep='=', _logger=None, with_timestamp=True) -> None:
    """
    画一个带时间戳的横线
    :param name: 中间的名称
    :param sep: 分隔符
    :return: 没有返回值，直接画一条线
    :param _logger: 指定输出的log文件
    :param with_timestamp: 是否带时间戳
    """
    seperator_len = 82
    if with_timestamp:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        middle = f' {name} {now} '
    else:
        middle = f' {name} '
    middle_width = get_display_width(middle)
    decoration_count = max(4, (seperator_len - middle_width) // 2)
    line = sep * decoration_count + middle + sep * decoration_count

    # 如果总长度不够，再补一个分隔符
    if get_display_width(line) < seperator_len:
        line += sep

    if _logger:
        _logger.debug(line)
    else:
        logger.debug(line)
    time.sleep(0.02)


logger = get_logger()

# 直接运行，查看使用案例
if __name__ == '__main__':
    # 输出日志信息
    logger.debug("调试信息，没有标记和颜色，等同于print")
    logger.info("提示信息，蓝色的，可以记录一些中间结果")
    # noinspection PyUnresolvedReferences
    logger.ok("完成提示，绿色的，通常表示成功和完成")
    logger.warning("警告信息，黄色的，通常表示警告")
    logger.error("错误信息，红色的，通常是报错的相关提示")
    logger.critical("重要提示，深红色。通常是非常关键的信息")
    divider('这个是我做的分割线的功能')
    divider('点点是可以换的', sep='*')
    divider('文字是居中的哦，英文和中文我尽量适配了。。。', sep='-')
    print('所有的日志都会在 `logs -> quantclasspro.log` 文件中，不会丢失～～～')
