import json
import time
import hmac
import hashlib
import base64
import urllib.parse
import httpx
import json
import logging
import asyncio
import pandas as pd
import time
from functools import wraps
from typing import Any, Dict, List, Optional, TypedDict, Callable, Tuple
from us_stock_wizard import StockRootDirectory
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


class DingTalkBot:
    """
    DingTalk Bot

    Example:
    ```
    import asyncio

    async def main():
        bot = DingTalkBot()
        response = await bot.send_msg('Hello, world!')

    asyncio.run(main())
    ```
    """

    def __init__(self):
        env = StockRootDirectory.env()
        self.apikey = env["DINGTALK_KEY"]
        self.secret = env["DINGTALK_SECRET"]

    def get_sign(self) -> Tuple[str, str]:
        # Get the sign based on secret
        timestamp = str(round(time.time() * 1000))
        secret_enc = self.secret.encode("utf-8")
        string_to_sign = "{}\n{}".format(timestamp, self.secret)
        string_to_sign_enc = string_to_sign.encode("utf-8")
        hmac_code = hmac.new(
            secret_enc, string_to_sign_enc, digestmod=hashlib.sha256
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign

    async def send_msg(self, msg: str) -> bool:
        timestamp, sign = self.get_sign()
        url = f"https://oapi.dingtalk.com/robot/send?access_token={self.apikey}&timestamp={timestamp}&sign={sign}"
        headers = {"Content-Type": "application/json"}
        body = {"msgtype": "text", "text": {"content": msg}}
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, data=json.dumps(body))
        result: dict = response.json()
        if result["errcode"] != 0:
            return False
        return True


def create_xlsx_file(multi_data: Dict[str, pd.DataFrame], file_path: str):
    """
    Create Customize Excel File

    Args:
        multi_data (Dict[str, pd.DataFrame]): key is table, value is dataframe
        file_path (str): file path, e.g. /path/to/file.xlsx
    """
    if not multi_data:
        logging.warning("No data to create file")
        return False
    if not file_path.endswith(".xlsx"):
        file_path = f"{file_path}.xlsx"
    writer = pd.ExcelWriter(file_path, engine="xlsxwriter")

    for table, data in multi_data.items():
        data.to_excel(writer, sheet_name=table)
        # workbook = writer.book
        worksheet = writer.sheets[table]
        worksheet.freeze_panes(1, 0)  # Freeze the first row

    writer.close()
    return True


def retry_decorator(
    retries: int = 2, delay: int = 1, failure_return_types: List[Any] = [None]
):
    """
    A decorator for retrying a sync function upon failure.

    This decorator will catch exceptions and also retry if the function's return value is within the
    specified `failure_return_types`. It will attempt to call the function a maximum of `retries` times,
    with a delay of `delay` seconds between each attempt.

    Parameters
    ----------
    retries : int, optional
        The maximum number of times to attempt to call the function, by default 3
    delay : int, optional
        The number of seconds to wait between each attempt, by default 3
    failure_return_types : List[Any], optional
        A list of return values that should be considered as failures and thus cause the function to be retried, by default [None]

    Returns
    -------
    Callable
        A wrapper around the decorated function, which adds the retry functionality

    Raises
    ------
    Exception
        The last exception raised by the decorated function if all retries failed
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_result = None
            last_exception = None
            for i in range(retries):
                try:
                    result = func(*args, **kwargs)
                    last_result = result  # save the result of each call
                    if result not in failure_return_types:
                        return result
                except Exception as e:
                    last_exception = e
                    print(
                        f"Attempt {i+1} failed with error: {e}. Retrying in {delay} seconds..."
                    )
                else:
                    print(f"Attempt {i+1} failed. Retrying in {delay} seconds...")
                time.sleep(delay)
            if last_exception is not None:
                raise last_exception
            return last_result  # return the result of the last call

        return wrapper

    return decorator


def async_retry_decorator(
    retries: int = 2, delay: int = 1, failure_return_types: List[Any] = [None]
):
    """
    A decorator for retrying an async function upon failure.

    This decorator will catch exceptions and also retry if the function's return value is within the
    specified `failure_return_types`. It will attempt to call the function a maximum of `retries` times,
    with a delay of `delay` seconds between each attempt.

    Parameters
    ----------
    retries : int, optional
        The maximum number of times to attempt to call the function, by default 3
    delay : int, optional
        The number of seconds to wait between each attempt, by default 3
    failure_return_types : List[Any], optional
        A list of return values that should be considered as failures and thus cause the function to be retried, by default [None]

    Returns
    -------
    Callable
        A wrapper around the decorated function, which adds the retry functionality

    Raises
    ------
    Exception
        The last exception raised by the decorated function if all retries failed
    """

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_result = None
            last_exception = None
            for i in range(retries):
                try:
                    result = await func(*args, **kwargs)
                    last_result = result  # save the result of each call
                    if result not in failure_return_types:
                        return result
                except Exception as e:
                    last_exception = e
                    print(
                        f"Attempt {i+1} failed with error: {e}. Retrying in {delay} seconds..."
                    )
                else:
                    print(f"Attempt {i+1} failed. Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
            if last_exception is not None:
                raise last_exception
            return last_result  # return the result of the last call

        return wrapper

    return decorator


class StockCommon:
    """
    Stock common utils
    """

    @staticmethod
    async def get_stock_list(conditions: dict = {}, format: str = "list") -> List[str]:
        """
        Get list of stocks from the database, based on the given conditions

        Args:
            conditions (dict, optional): Conditions to filter the stocks. Defaults to {}.
            format (str, optional): Format of the output. Defaults to "list". / "df"
        """
        stocks = await StockDbUtils.read(table="Tickers", where=conditions)
        stock_df = StockCommon.convert_to_dataframe(stocks)
        stock_df = stock_df[~stock_df["ticker"].str.contains("\^")]
        if format == "list":
            return stock_df["ticker"].tolist()
        if format == "df":
            return stock_df
        raise Exception(f"Invalid format: {format}")

    @staticmethod
    def convert_to_dataframe(data: list) -> pd.DataFrame:
        """
        Convert the given data from database to dataframe
        """
        _data = [d.dict() for d in data]
        return pd.DataFrame(_data)
