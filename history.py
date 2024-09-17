# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo History Objects
@author: Jack Kirby Cook

"""

import logging
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime as Datetime

from finance.variables import Symbol
from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebHTML
from webscraping.weburl import WebURL
from support.mixins import Sizing

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["YahooBarsDownloader"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = ""
__logger__ = logging.getLogger(__name__)


class Parsers(object):
    prices = {column: lambda x: np.float32(str(x).replace(",", "")) for column in ("open", "close", "high", "low", "price")}
    volumes = {column: lambda x: np.int64(str(x).replace(",", "")) for column in ("volume",)}
    dates = {column: pd.to_datetime for column in ("date",)}

    @staticmethod
    def history(dataframe):
        dataframe.columns = [str(column).split(" ")[0].lower() for column in dataframe.columns]
        dataframe = dataframe.rename(columns={"adj": "price"}, inplace=False)
        dataframe = dataframe[~dataframe["open"].apply(str).str.contains("Dividend") & ~dataframe["open"].apply(str).str.contains("Split")]
        return dataframe


class YahooTechnicalURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://finance.yahoo.com"
    def path(cls, *args, ticker, **kwargs): return f"/quote/{str(ticker)}/history"
    def parms(cls, *args, dates, **kwargs):
        start = Datetime.combine(dates.minimum, Datetime.min.time()).timestamp()
        stop = Datetime.combine(dates.maximum, Datetime.min.time()).timestamp()
        return {"period1": start, "period2": stop, "frequency": "1d", "includeAdjustedClose": "true"}


class YahooTechnicalData(WebHTML.Table, locator=r"//table", key="history", parser=Parsers.history): pass
class YahooTechnicalPage(WebBrowserPage):
    def __call__(self, *args, ticker, dates, **kwargs):
        curl = YahooTechnicalURL(ticker=ticker, dates=dates)
        self.load(str(curl.address), params=dict(curl.query))
        self.pageend()
        content = YahooTechnicalData(self.source)
        table = content(*args, **kwargs)
        bars = self.bars(table, *args, ticker=ticker, **kwargs)
        return bars

    @staticmethod
    def bars(dataframe, *args, ticker, **kwargs):
        function = lambda parsers: lambda columns: {column: parser(columns[column]) for column, parser in parsers.items()}
        prices = dataframe.apply(function(Parsers.prices), axis=1, result_type="expand")
        volumes = dataframe.apply(function(Parsers.volumes), axis=1, result_type="expand")
        dates = dataframe.apply(function(Parsers.dates), axis=1, result_type="expand")
        dataframe = pd.concat([dates, prices, volumes], axis=1)
        dataframe = dataframe.sort_values("date", axis=0, ascending=True, inplace=False)
        dataframe["ticker"] = str(ticker).upper()
        return dataframe


class YahooTechnicalDownloader(Sizing, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = YahooTechnicalPage(*args, **kwargs)
        self.__logger = __logger__

    @abstractmethod
    def download(self, *args, **kwargs): pass

    @property
    def logger(self): return self.__logger
    @property
    def page(self): return self.__page


class YahooBarsDownloader(YahooTechnicalDownloader):
    def download(self, symbol, *args, dates, **kwargs):
        assert isinstance(symbol, Symbol)
        bars = self.page(*args, ticker=symbol.ticker, dates=dates, **kwargs)
        assert isinstance(bars, pd.DataFrame)
        size = self.size(bars)
        string = f"Downloaded: {repr(self)}|{str(symbol)}[{size:.0f}]"
        self.logger.info(string)
        return bars





