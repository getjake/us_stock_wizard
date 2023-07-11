import logging
import asyncio
from typing import List, Optional
import pandas as pd
import time
from functools import wraps
from typing import Any, Dict, List, Optional, TypedDict, Callable
from us_stock_wizard.database.db_utils import StockDbUtils, DbTable


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
    async def get_stock_list(conditions: dict = {}) -> List[str]:
        """
        Get list of stocks from the database, based on the given conditions
        """
        stocks = await StockDbUtils.read(table="Tickers", where=conditions)
        stock_df = StockCommon.convert_to_dataframe(stocks)
        stock_df = stock_df[~stock_df["ticker"].str.contains("\^")]
        return stock_df["ticker"].tolist()

    @staticmethod
    def convert_to_dataframe(data: list) -> pd.DataFrame:
        """
        Convert the given data from database to dataframe
        """
        _data = [d.dict() for d in data]
        return pd.DataFrame(_data)
