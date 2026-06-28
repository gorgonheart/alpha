import os
import shutil
import time
from datetime import datetime
from glob import glob
from pathlib import Path

import pandas as pd

from core.utils.log_kit import logger


class CandleFileReader:

    def __init__(self, base_dir, save_type):
        """
        初始化，设定读写根目录
        :param base_dir: 读写根目录
        :param save_type: 保存类型
        """
        self.base_dir: Path = Path(base_dir)
        self.save_type = save_type
        if save_type == 'feather':
            self.ext = 'fea'
        elif save_type == 'parquet':
            self.ext = 'pqt'
        elif save_type == 'pickle':
            self.ext = 'pkl'
        elif save_type == 'compressed_pickle':
            self.ext = 'pkl.zst'
        else:
            raise ValueError(f'Save type {save_type} not supported, can only accept feather and parquet')

    def format_ready_file_path(self, symbol, run_time: datetime):
        """
        获取 ready file 文件路径, ready file 为每周期 K线文件锁
        ready file 文件名形如 {symbol}_{runtime年月日}_{runtime_时分秒}.ready
        :param symbol: 币种
        :param run_time: 运行时间
        :return: ready file 文件路径
        """
        run_time_str = str(int(run_time.timestamp()))
        name = f'{symbol}_{run_time_str}.ready'
        file_path = os.path.join(self.base_dir, name)
        return file_path

    def format_data_file_path(self, symbol):
        name = f'{symbol}.{self.ext}'
        file_path = os.path.join(self.base_dir, name)
        return file_path

    def check_ready(self, symbol, run_time):
        """
        检查 symbol 对应的 ready file 是否存在，如存在，则表示 run_time 周期 K线已获取并写入 Feather
        :param symbol: 币种
        :param run_time: 运行时间
        :return: True or False
        """
        ready_file_path = self.format_ready_file_path(symbol, run_time)
        return os.path.exists(ready_file_path)

    def read_candle(self, symbol) -> pd.DataFrame:
        """
        读取 symbol 对应的 K线
        :param symbol:  币种
        :return:        DataFrame
        """
        df_path = self.format_data_file_path(symbol)
        _t0 = time.perf_counter()
        if self.save_type == 'feather':
            df = pd.read_feather(df_path)
        elif self.save_type == 'parquet':
            df = pd.read_parquet(df_path)
        elif self.save_type == 'pickle' or self.save_type == 'compressed_pickle':
            df = pd.read_pickle(df_path)
        else:
            return None
        _dt = time.perf_counter() - _t0
        try:
            _mb = os.path.getsize(df_path) / 1e6
            logger.debug(f'[io-stat] read {self.save_type} {symbol} dir={self.base_dir.name} '
                         f'size={_mb:.1f}MB took={_dt * 1000:.0f}ms')
        except OSError:
            pass
        return df

    def has_symbol(self, symbol) -> bool:
        """
        检查某 symbol 数据文件是否存在
        :param symbol:  币种
        :return:        True or False
        """
        df_path = self.format_data_file_path(symbol)
        return os.path.exists(df_path)

    def get_all_symbols(self):
        """
        获取当前所有 symbol
        :return:
        """
        paths = glob(os.path.join(self.base_dir, f'*.{self.ext}'))
        return [os.path.basename(p).split('.')[0] for p in paths]


class CandleFileManager(CandleFileReader):

    def clear_all(self):
        """
        清空历史文件（如有），并创建根目录
        :return:
        """
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)

    def save_data_file(self, symbol, df: pd.DataFrame):
        df_path = self.format_data_file_path(symbol)
        if os.path.exists(df_path):
            os.remove(df_path)
        _t0 = time.perf_counter()
        if self.save_type == 'feather':
            df = df.reset_index(drop=True)
            df.to_feather(df_path)
        elif self.save_type == 'parquet':
            df.to_parquet(df_path)
        elif self.save_type == 'pickle':
            if isinstance(df, pd.DataFrame):
                df.to_pickle(df_path)
            else:
                pd.to_pickle(df, df_path)
        elif self.save_type == 'compressed_pickle':
            if isinstance(df, pd.DataFrame):
                df.to_pickle(df_path)
            else:
                raise ValueError(f'Symbol {symbol} if of type {type(df)}, cannot save as compressed_pickle')
        _dt = time.perf_counter() - _t0
        try:
            _mb = os.path.getsize(df_path) / 1e6
            _rows = len(df) if hasattr(df, '__len__') else -1
            logger.debug(f'[io-stat] write {self.save_type} {symbol} dir={self.base_dir.name} '
                         f'rows={_rows} size={_mb:.1f}MB took={_dt * 1000:.0f}ms')
        except OSError:
            pass

    def set_candle(self, symbol, run_time, df: pd.DataFrame):
        """
        设置K线，首先将新的K线 DataFrame 写入 Feather，然后删除旧 ready file，并生成新 ready file
        :param symbol:  币种
        :param run_time:  运行时间
        :param df:      DataFrame
        :return:
        """
        self.save_data_file(symbol, df)

        self.save_ready(symbol, run_time)

    def save_ready(self, symbol, run_time):
        old_ready_file_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ready'))
        for p in old_ready_file_paths:
            os.remove(p)

        if run_time is not None:
            ready_file_path = self.format_ready_file_path(symbol, run_time)
            with open(ready_file_path, 'w') as fout:
                fout.write(str(datetime.now().timestamp()))

    def update_candle(self, symbol, run_time, df_new: pd.DataFrame, num_candles):
        """
        使用新获取的 K 线，更新 symbol 对应 K 线文件，主要用于每周期 K 线更新
        :param symbol:  币种
        :param run_time:  运行时间
        :param df_new:  新获取的 K 线
        :param num_candles: 获取的 K 线数量
        :return:        DataFrame
        """
        if self.has_symbol(symbol):
            df_old = self.read_candle(symbol)
            if df_old.empty:
                df = df_new
            else:
                # print(df_old['candle_begin_time'].iloc[0], df_old['candle_begin_time'].iloc[-1],
                #       df_new['candle_begin_time'].iloc[0], df_new['candle_begin_time'].iloc[-1])
                df: pd.DataFrame = pd.concat([df_old, df_new])
        else:
            df = df_new
        df.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
        df.sort_values('candle_begin_time', inplace=True)
        if num_candles is not None:
            df = df.tail(num_candles)
        self.set_candle(symbol, run_time, df)

        return df

    def update_candle_incremental(self, symbol, run_time, df_new: pd.DataFrame, num_candles):
        if not self.has_symbol(symbol):
            return self.update_candle(symbol, run_time, df_new, num_candles)

        df_old = self.read_candle(symbol)
        if df_old.empty or df_new.empty:
            return self.update_candle(symbol, run_time, df_new, num_candles)

        last_old_time = df_old['candle_begin_time'].max()
        first_new_time = df_new['candle_begin_time'].min()
        if first_new_time <= last_old_time:
            return self.update_candle(symbol, run_time, df_new, num_candles)

        df_old = df_old.copy(deep=True)
        df_old.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
        df_old.sort_values('candle_begin_time', inplace=True)

        df_new = df_new.copy(deep=True)
        df_new.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
        df_new.sort_values('candle_begin_time', inplace=True)

        if num_candles is None:
            df = pd.concat([df_old, df_new])
        else:
            keep_old_rows = max(num_candles - len(df_new), 0)
            df_old = df_old.tail(keep_old_rows) if keep_old_rows else df_old.iloc[0:0]
            df = pd.concat([df_old, df_new]).tail(num_candles)

        self.set_candle(symbol, run_time, df)
        return df

    def remove_symbol(self, symbol):
        """
        移除 symbol，包括删除对应的数据文件和 ready file
        :param symbol:  币种
        :return:
        """
        old_ready_file_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ready'))
        for p in old_ready_file_paths:
            os.remove(p)
        df_path = self.format_data_file_path(symbol)
        if os.path.exists(df_path):
            os.remove(df_path)
