# -*- coding: utf-8 -*-
"""
Created on Weds Jul 19 2023
@name:   Yahoo History Objects
@author: Jack Kirby Cook

"""

import numpy as np
import xarray as xr
import pandas as pd
from datetime import datetime as Datetime

from webscraping.weburl import WebURL
from webscraping.webnodes import WebHTML
from webscraping.webpages import WebBrowserPage
from support.pipelines import Downloader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["YahooHistoryDownloader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


datetime_parser = lambda x: np.datetime64(Datetime.strptime(x, "%b %d, %Y").date(), "D")
volume_parser = lambda x: np.int64(str(x).replace(",", ""))


class YahooHistoryURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://finance.yahoo.com"
    def path(cls, *args, ticker, **kwargs): return "/quote/{ticker}/history".format(ticker=str(ticker))

    def parms(cls, *args, ticker, history, **kwargs):
        start, stop = list(map(lambda x: int(x.timestamp()), history))
        constants = {"interval": "1d", "filter": "history", "frequency": "1d", "includeAdjustedClose": "true"}
        return {"period1": start, "period2": stop, **constants}


class YahooHistoryData(WebHTML.Table, locator=r"//table[@data-test='historical-prices']", parameters={"index": None, "header": 0}):
    @staticmethod
    def parser(dataframe, *args, ticker, **kwargs):
        dataframe = dataframe.iloc[:-1, :]
        dataframe.columns = [str(column).replace("*", "").lower() for column in dataframe.columns]
        dataframe = dataframe.rename(columns={"adj close": "price"}, inplace=False)
        dataframe = dataframe[~dataframe["open"].str.contains("Dividend") & ~dataframe["open"].str.contains("Split")]
        if dataframe.empty:
            return pd.DataFrame()
        dataframe["date"] = dataframe["date"].apply(datetime_parser)
        for column in ["open", "close", "high", "low", "price"]:
            dataframe[column] = dataframe[column].apply(np.float32)
        dataframe["volume"] = dataframe["volume"].apply(volume_parser)
        dataframe["ticker"] = str(ticker)
        dataset = xr.Dataset.from_dataframe(dataframe)
        dataset = dataset.squeeze("ticker")
        return dataset


class YahooHistoryPage(WebBrowserPage):
    def execute(self, *args, **kwargs):
        table = YahooHistoryData(self.source)(*args, **kwargs)
        if table.empty:
            return
        current, updated = 0, len(table)
        while updated != current:
            self.pageEnd()
            table = YahooHistoryData(self.source)(*args, **kwargs)
            current, updated = updated, len(table)


class YahooHistoryDownloader(Downloader, pages={"history": YahooHistoryData}):
    def execute(self, ticker, *args, history, **kwargs):
        curl = YahooHistoryURL(ticker=ticker, history=history)
        self.pages["history"].load(str(curl))
        self.pages["history"].run(ticker=ticker)
        source = self.pages["history"].source
        history = YahooHistoryData(source)(ticker=ticker)
        yield ticker, history




