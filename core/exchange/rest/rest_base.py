"""交易所 REST API 基类。

封装与交易所 HTTP API 交互的通用逻辑，提供统一的请求处理接口。

说明：本模块从 okx 分支 salvage 而来，已移除对 ``core.utils.enums.Exchange``
枚举的依赖，``exchange`` 现为可选字符串标识（例如 ``"okx"``）。
"""

import json

import aiohttp


class RestRequestException(Exception):
    """交易所请求异常类。

    用于处理交易所请求过程中发生的异常情况。
    """

    def __init__(self, exchange: str, message: str):
        """初始化异常实例。

        Args:
            exchange: 交易所标识字符串
            message: 异常消息内容
        """
        self.message = message
        self.exchange = exchange

    def __str__(self):
        """返回异常的字符串表示。

        Returns:
            异常的描述信息字符串
        """
        return f"RestRequestException {self.exchange}: {self.message}"


class RestAPIException(Exception):
    """交易所API调用异常类。

    用于处理交易所API调用过程中返回的错误响应。
    """

    def __init__(self, exchange: str, status_code: int, text: str):
        """初始化API异常实例。

        Args:
            exchange: 交易所标识字符串
            status_code: HTTP响应状态码
            text: 响应文本内容
        """
        self.exchange = exchange
        self.status_code = status_code
        self.code = 0
        self.message = text

        try:
            # 尝试将响应文本解析为JSON格式
            json_res = json.loads(text)
        except ValueError:
            return

        # 从JSON响应中提取错误代码
        self.code = json_res.get("code", 0)
        # 从JSON响应中提取错误消息
        self.message = json_res.get("msg", text)

    def __str__(self):  # pragma: no cover
        """返回API异常的字符串表示。

        Returns:
            包含交易所、状态码、错误码和消息的描述信息
        """
        return f"RestAPIException(交易所={self.exchange}, 状态码={self.status_code}, 错误码={self.code}): {self.message}"


class OkxAPIException(Exception):
    """OKX 业务层错误异常类。

    OKX v5 在 HTTP 200 的响应体里用 ``code`` 字段表达业务层错误
    （如限速 50011、参数错误、instId 无效等），``code != "0"`` 即为错误。
    本异常携带 OKX 的 ``code`` 与 ``msg``，供上层（含 ``async_retry_getter``
    的重试判定）感知，避免错误响应被当成空成功静默吞掉。
    """

    def __init__(self, code, msg: str):
        """初始化 OKX 业务异常实例。

        Args:
            code: OKX 业务错误码（字符串或数字），非 "0" 即错误。
            msg: OKX 返回的错误消息。
        """
        self.code = code
        self.msg = msg
        super().__init__(f"OkxAPIException(code={code}): {msg}")

    def __str__(self):  # pragma: no cover
        return f"OkxAPIException(code={self.code}): {self.msg}"


class RestBaseApi:
    """交易所REST API基类。

    封装与交易所API交互的通用逻辑，提供统一的请求处理接口。
    """

    def __init__(self, exchange: str, session: aiohttp.ClientSession, proxy: str):
        """初始化REST API基类实例。

        Args:
            exchange: 交易所标识字符串
            session: 用于发送HTTP请求的异步会话对象
            proxy: 代理服务器地址
        """
        self.exchange = exchange
        self.session = session
        self.proxy = proxy

    async def _handle_response(self, response: aiohttp.ClientResponse):
        """处理HTTP响应。

        检查响应状态码并解析JSON数据。如果状态码非2xx则抛出API异常，
        如果响应无法解析为JSON则抛出请求异常。

        Args:
            response: HTTP响应对象

        Returns:
            解析后的JSON数据字典

        Raises:
            RestAPIException: 响应状态码非2xx时抛出
            RestRequestException: 响应无法解析为JSON时抛出
        """
        if not str(response.status).startswith("2"):
            # 状态码非2xx时抛出API异常
            raise RestAPIException(self.exchange, response.status, await response.text())
        try:
            # 尝试将响应解析为JSON格式
            return await response.json()
        except ValueError:
            # JSON解析失败时抛出请求异常
            txt = await response.text()
            raise RestRequestException(self.exchange, f"Invalid Response: {txt}")

    async def _aio_get(self, url, params):
        """发送异步GET请求。

        Args:
            url: 请求的URL地址
            params: 请求参数字典

        Returns:
            解析后的JSON响应数据
        """
        if params is None:
            params = {}
        # 发送GET请求并处理响应
        async with self.session.get(url, params=params, proxy=self.proxy) as resp:
            return await self._handle_response(resp)

    async def _aio_post(self, url, params):
        """发送异步POST请求。

        Args:
            url: 请求的URL地址
            params: 请求参数字典

        Returns:
            解析后的JSON响应数据
        """
        # 发送POST请求并处理响应
        async with self.session.post(url, data=params, proxy=self.proxy) as resp:
            return await self._handle_response(resp)
