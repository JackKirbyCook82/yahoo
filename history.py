# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo History Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from datetime import datetime as Datetime

from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebHTML
from webscraping.weburl import WebURL
from support.pipelines import Processor

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["YahooHistoryDownloader"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = ""


bars_index = {"date": np.datetime64}
bars_columns = {"high": np.float32, "low": np.float32, "open": np.float32, "close": np.float32, "price": np.float32, "volume": np.float32}
bars_header = Header(pd.DataFrame, index=list(bars_index.keys()), columns=list(bars_columns.keys()), ascending="date")
history_headers = dict(bars=bars_header)
history_locator = r"//table[@class='table svelte-ewueuo']"
volume_parser = lambda x: np.int64(str(x).replace(",", ""))
price_parser = lambda x: np.float32(str(x).replace(",", ""))


def history_parser(dataframe):
    dataframe.columns = [str(column).split(" ")[0].lower() for column in dataframe.columns]
    dataframe = dataframe.rename(columns={"adj": "price"}, inplace=False)
    dataframe = dataframe[~dataframe["open"].apply(str).str.contains("Dividend") & ~dataframe["open"].apply(str).str.contains("Split")]
    return dataframe


class YahooHistoryURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://finance.yahoo.com"
    def path(cls, *args, ticker, **kwargs): return f"/quote/{str(ticker)}/history"
    def parms(cls, *args, dates, **kwargs):
        start = Datetime.combine(dates.minimum, Datetime.min.time()).timestamp()
        stop = Datetime.combine(dates.maximum, Datetime.min.time()).timestamp()
        return {"period1": start, "period2": stop, "frequency": "1d", "includeAdjustedClose": "true"}


class YahooHistoryData(WebHTML.Table, locator=history_locator, key="history", parser=history_parser): pass
class YahooHistoryPage(WebBrowserPage):
    def __call__(self, ticker, *args, dates, **kwargs):
        curl = YahooHistoryURL(ticker=ticker, dates=dates)
        self.load(str(curl.address), params=dict(curl.query))
        self.pageend()
        content = YahooHistoryData(self.source)
        table = content(*args, **kwargs)
        bars = self.bars(table, *args, ticker=ticker, **kwargs)
        return bars

    @staticmethod
    def bars(dataframe, *args, ticker, **kwargs):
        dataframe["date"] = dataframe["date"].apply(pd.to_datetime)
        for column in ["open", "close", "high", "low", "price"]:
            dataframe[column] = dataframe[column].apply(price_parser)
        dataframe["volume"] = dataframe["volume"].apply(volume_parser)
        dataframe["ticker"] = str(ticker).upper()
        return dataframe


class YahooHistoryDownloader(Processor, title="Downloaded"):
    def __init__(self, *args, feed, name=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        self.__history = YahooHistoryPage(*args, feed=feed, **kwargs)

    @Query(arguments=["ticker"], headers=history_headers)
    def execute(self, *args, ticker, dates, **kwargs):
        bars = self.history(ticker, *args, dates=dates, **kwargs)
        bars = bars.sort_values("date", axis=0, ascending=True, inplace=False)
        yield dict(bars=bars)

    @property
    def history(self): return self.__history



