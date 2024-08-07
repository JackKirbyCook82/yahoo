# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo History Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from datetime import datetime as Datetime

from finance.variables import Variables
from support.pipelines import Processor
from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebHTML
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["YahooHistoryDownloader"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = ""


history_formatter = lambda self, *, results, elapsed, **kw: f"{str(self.title)}: {repr(self)}|{str(results[Variables.Querys.SYMBOL])}[{elapsed:.02f}s]"
volume_parser = lambda x: np.int64(str(x).replace(",", ""))
price_parser = lambda x: np.float32(str(x).replace(",", ""))
history_locator = r"//table"


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
    def __call__(self, *args, ticker, dates, **kwargs):
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
        dataframe = dataframe.sort_values("date", axis=0, ascending=True, inplace=False)
        dataframe["ticker"] = str(ticker).upper()
        return dataframe


class YahooHistoryDownloader(Processor, title="Downloaded", formatter=history_formatter):
    def __init__(self, *args, feed, name=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        bars = YahooHistoryPage(*args, feed=feed, **kwargs)
        self.__downloads = {Variables.Technicals.BARS: bars}

    def processor(self, contents, *args, dates, **kwargs):
        ticker = contents[Variables.Querys.SYMBOL].ticker
        bars = self.downloads[Variables.Technicals.BARS](*args, ticker=ticker, dates=dates, **kwargs)
        technicals = {Variables.Technicals.BARS: bars}
        yield contents | technicals

    @property
    def downloads(self): return self.__downloads



