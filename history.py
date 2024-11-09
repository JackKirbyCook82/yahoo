# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo History Objects
@author: Jack Kirby Cook

"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime as Datetime

from finance.variables import Variables, Querys
from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebHTML
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Logging
from support.meta import RegistryMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["YahooTechnicalDownloader"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = ""
__logger__ = logging.getLogger(__name__)


class YahooHistoryParsers(object):
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


class YahooTechnicalData(WebHTML.Table, locator=r"//table", key="history", parser=YahooHistoryParsers.history):
    pass


class YahooTechnicalPage(WebBrowserPage, metaclass=RegistryMeta): pass
class YahooBarsPage(YahooTechnicalPage, register=Variables.Technicals.BARS):
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
        prices = dataframe.apply(function(YahooHistoryParsers.prices), axis=1, result_type="expand")
        volumes = dataframe.apply(function(YahooHistoryParsers.volumes), axis=1, result_type="expand")
        dates = dataframe.apply(function(YahooHistoryParsers.dates), axis=1, result_type="expand")
        dataframe = pd.concat([dates, prices, volumes], axis=1)
        dataframe = dataframe.sort_values("date", axis=0, ascending=True, inplace=False)
        dataframe["ticker"] = str(ticker).upper()
        return dataframe


class YahooTechnicalDownloader(Logging, Sizing, Emptying):
    def __init__(self, *args, technical=Variables.Technicals.BARS, **kwargs):
        assert technical in list(Variables.Technicals)
        super().__init__(*args, **kwargs)
        self.__page = YahooTechnicalPage[technical](*args, **kwargs)

    def execute(self, symbol, *args, dates, **kwargs):
        assert isinstance(symbol, Querys.Symbol)
        parameters = dict(ticker=symbol.ticker, dates=dates)
        bars = self.download(*args, **parameters, **kwargs)
        size = self.size(bars)
        string = f"Downloaded: {repr(self)}|{str(symbol)}[{size:.0f}]"
        self.logger.info(string)
        if self.empty(bars): return
        return bars

    def download(self, *args, ticker, dates, **kwargs):
        bars = self.page(*args, ticker=ticker, dates=dates, **kwargs)
        assert isinstance(bars, pd.DataFrame)
        return bars

    @property
    def page(self): return self.__page






