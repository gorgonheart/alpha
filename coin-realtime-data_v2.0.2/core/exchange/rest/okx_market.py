"""OKX行情数据API封装.

基于 OKX v5 API，为所有类型交易产品提供统一的API接口。

说明：本模块从 okx 分支 salvage 而来，已移除对 ``core.utils.enums.Exchange``
枚举的依赖，基类 ``exchange`` 标识改用字符串 ``"okx"``。
"""

import aiohttp

from core.exchange.rest.rest_base import OkxAPIException, RestBaseApi

# OKX K线接口单次返回上限（/market/candles 与 /market/history-candles 均为 300）
CANDLES_LIMIT_CAP = 300

# OKX 业务层成功码（HTTP 200 + code == "0" 才算成功）
OKX_SUCCESS_CODE = ("0", 0)


class OkxMarketApi(RestBaseApi):
    """OKX行情数据API基类.

    继承自RestBaseApi，提供OKX交易所行情数据的统一访问接口。
    """

    PREFIX = "https://www.okx.com/api/v5"

    def __init__(self, session: aiohttp.ClientSession, proxy: str):
        """初始化OKX行情API客户端.

        Args:
            session: aiohttp客户端会话
            proxy: 代理服务器地址
        """
        super().__init__("okx", session, proxy)

    def _extract_data(self, resp: dict) -> None | list | dict:
        """校验 OKX 业务层错误码后提取 data 字段。

        OKX v5 即便业务出错也返回 HTTP 200，错误信息体现在响应体的
        ``code`` 字段（``code != "0"`` 即错误，如限速 50011、参数错误等）。
        若仅看 HTTP 状态码，这些错误会被当成空成功静默吞掉、并隐藏 429。

        Args:
            resp: API 响应的原始数据字典。

        Returns:
            ``resp["data"]`` 的内容；若不存在 data 字段则返回 None。

        Raises:
            OkxAPIException: 当 ``resp["code"]`` 非 "0" 时抛出，携带 code + msg。
        """
        code = resp.get("code")
        if code not in OKX_SUCCESS_CODE:
            raise OkxAPIException(code, resp.get("msg", ""))
        if "data" in resp:
            return resp["data"]
        return None

    def handle_resp_data(self, data: dict) -> None | list | dict:
        """处理API响应数据（保留旧名作为 :meth:`_extract_data` 的别名）。

        从OKX API响应中提取data字段的内容，并先校验业务层错误码。

        Args:
            data: API响应的原始数据字典

        Returns:
            提取的数据内容，如果不存在data字段则返回None

        Raises:
            OkxAPIException: 当业务层 code 非 "0" 时抛出。
        """
        return self._extract_data(data)

    async def aioreq_instruments(self, **kwargs) -> list[dict] | None:
        """获取交易产品基础信息.

        获取所有可交易产品的详细信息列表。
        限速：20次/2s，按IP + 产品类型限制。

        Args:
            **kwargs: 请求参数
                instType (str): 产品类型，必填
                    - SPOT: 币币交易
                    - MARGIN: 币币杠杆
                    - SWAP: 永续合约
                    - FUTURES: 交割合约
                    - OPTION: 期权
                instFamily (str, optional): 交易品种，仅适用于交割/永续/期权
                instId (str, optional): 产品ID

        Returns:
            list: 交易产品信息列表，包含产品ID、类型、状态、最小交易量等
        """
        url = f"{self.PREFIX}/public/instruments"
        data = await self._aio_get(url, kwargs)
        return self.handle_resp_data(data)

    async def aioreq_candles(self, history: bool = False, **kwargs) -> list[list] | None:
        """获取交易产品K线数据.

        按指定粒度返回K线数据，每个粒度最多可获取最近1440条记录。
        限速：
            /history-candles 接口(history=True): 20次/2s，按IP限制。
            /candles 接口(history=False): 40次/2s，按IP限制。

        Args:
            history (bool): 是否获取历史K线数据，默认False
            **kwargs: 请求参数
                instId (str): 产品ID，如BTC-USD-SWAP
                bar (str): K线粒度，支持1m/3m/5m/15m/30m/1H/2H/4H/6H/12H/1D/1W/1M/3M/6M/1Y
                after (str, optional): 请求此时间戳之前（更旧的数据）的分页内容，Unix毫秒时间戳
                before (str, optional): 请求此时间戳之后（更新的数据）的分页内容，Unix毫秒时间戳
                limit (int | str, optional): 返回结果数量，最大300，默认100；超过300会被裁剪为300

        Returns:
            list: K线数据列表，每个元素包含以下字段
                ts (str): 开始时间，Unix毫秒时间戳
                o (str): 开盘价格
                h (str): 最高价格
                l (str): 最低价格
                c (str): 收盘价格
                vol (str): 交易量（张数或交易货币数量）
                volCcy (str): 交易量（币为单位）
                volCcyQuote (str): 交易量（计价货币为单位）
                confirm (str): K线状态，0=未完结，1=已完结
        """
        # 将 limit 裁剪到 OKX 单次上限 300（接受 int 或数字字符串）
        if "limit" in kwargs and kwargs["limit"] is not None:
            limit = int(kwargs["limit"])
            if limit > CANDLES_LIMIT_CAP:
                limit = CANDLES_LIMIT_CAP
            kwargs["limit"] = limit

        if history:
            url = f"{self.PREFIX}/market/history-candles"
        else:
            url = f"{self.PREFIX}/market/candles"
        data = await self._aio_get(url, kwargs)
        return self.handle_resp_data(data)

    async def aioreq_funding_rate(self, **kwargs) -> list[dict] | None:
        """获取永续合约资金费率.

        获取当前资金费率信息。
        限速：10次/2s，按IP + 产品ID限制。

        Args:
            **kwargs: 请求参数
                instId (str): 产品ID，如BTC-USD-SWAP，或ANY获取所有永续合约费率

        Returns:
            list: 资金费率信息列表，每个元素包含以下字段
                instId (str): 产品ID
                fundingRate (str): 资金费率
                fundingTime (str): 资金费时间，Unix毫秒时间戳
                nextFundingTime (str): 下一期资金费时间，Unix毫秒时间戳
        """
        url = f"{self.PREFIX}/public/funding-rate"
        data = await self._aio_get(url, kwargs)
        return self.handle_resp_data(data)

    async def aioreq_funding_rate_history(self, **kwargs) -> list[dict] | None:
        """获取永续合约历史资金费率.

        获取历史资金费率，最多返回近三个月数据。
        限速：10次/2s，按IP + 产品ID限制。

        Args:
            **kwargs: 请求参数
                instId (str): 产品ID，如BTC-USD-SWAP，仅适用于永续
                before (str, optional): 请求此时间戳之后（更新的数据）的分页内容，传的值为对应接口的fundingTime
                after (str, optional): 请求此时间戳之前（更旧的数据）的分页内容，传的值为对应接口的fundingTime
                limit (str, optional): 分页返回的结果集数量，最大为400，不填默认返回400条

        Returns:
            list: 历史资金费率信息列表，每个元素包含以下字段
                instId (str): 产品ID
                fundingRate (str): 资金费率
                realizedRate (str): 实际资金费率
                fundingTime (str): 资金费时间，Unix毫秒时间戳
                instType (str): 产品类型
                method (str): 计算方法
                formulaType (str): 公式类型
        """
        url = f"{self.PREFIX}/public/funding-rate-history"
        data = await self._aio_get(url, kwargs)
        return self.handle_resp_data(data)
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
