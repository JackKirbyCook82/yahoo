# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo Technical Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from datetime import datetime as Datetime

from finance.variables import Querys, Variables
from webscraping.webpages import WebBrowserPage
from webscraping.webdatas import WebHTMLs
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["YahooHistoryDownloader"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = ""


class YahooTechnicalURL(WebURL, domain="https://finance.yahoo.com"):
    @staticmethod
    def path(*args, technical, ticker, **kwargs): return ["quote", f"{str(ticker)}", f"{str(technical)}"]
    @staticmethod
    def parms(*args, dates, **kwargs):
        start = Datetime.combine(dates.minimum, Datetime.min.time()).timestamp()
        stop = Datetime.combine(dates.maximum, Datetime.min.time()).timestamp()
        return {"period1": start, "period2": stop, "frequency": "1d", "includeAdjustedClose": "true"}


class YahooTechnicalData(WebHTMLs.Table, locator=r"//table"): pass
class YahooHistoryData(YahooTechnicalData, key="history"):
    prices = {column: lambda x: np.float32(str(x).replace(",", "")) for column in ["open", "close", "high", "low", "price"]}
    volumes = {column: lambda x: np.int64(str(x).replace(",", "")) for column in ["volume"]}
    dates = {column: pd.to_datetime for column in ["date"]}

    def execute(self, dataframe, *args, ticker, **kwargs):
        assert isinstance(dataframe, pd.DataFrame)
        function = lambda parsers: lambda columns: {column: parser(columns[column]) for column, parser in parsers.items()}
        prices = dataframe.apply(function(self.prices), axis=1, result_type="expand")
        volumes = dataframe.apply(function(self.volumes), axis=1, result_type="expand")
        dates = dataframe.apply(function(self.dates), axis=1, result_type="expand")
        dataframe = pd.concat([dates, prices, volumes], axis=1)
        dataframe = dataframe.sort_values("date", axis=0, ascending=True, inplace=False)
        dataframe["ticker"] = str(ticker).upper()
        return dataframe

    @staticmethod
    def parser(dataframe):
        assert isinstance(dataframe, pd.DataFrame)
        dataframe.columns = [str(column).split(" ")[0].lower() for column in dataframe.columns]
        dataframe = dataframe.rename(columns={"adj": "price"}, inplace=False)
        dividend = ~dataframe["open"].apply(str).str.contains("Dividend")
        split = ~dataframe["open"].apply(str).str.contains("Split")
        dataframe = dataframe[dividend & split]
        return dataframe


class YahooTechnicalPage(WebBrowserPage):
    def execute(self, *args, technical, ticker, dates, **kwargs):
        parameters = dict(technical=technical, ticker=ticker, dates=dates)
        url = YahooTechnicalURL(**parameters)
        self.load(url)
        self.source.pageend()
        technicals = self[str(technical)](*args, **kwargs)
        return technicals

class YahooHistoryPage(YahooTechnicalPage, data=YahooHistoryData):
    pass


class YahooTechnicalDownloader(Logging, Sizing, Emptying):
    def __init__(self, *args, technical, query, **kwargs):
        assert technical in list(Variables.Technicals)
        try: super().__init__(*args, **kwargs)
        except TypeError: super().__init__()
        self.__page = YahooTechnicalPage(*args, **kwargs)
        self.__technical = technical
        self.__query = query

    def execute(self, symbol, *args, dates, **kwargs):
        if symbol is None: return
        symbol = self.query(symbol)
        parameters = dict(technical=self.technical, ticker=symbol.ticker, dates=dates)
        technicals = self.download(*args, **parameters, **kwargs)
        size = self.size(technicals)
        string = f"Downloaded: {repr(self)}|{str(symbol)}[{int(size):.0f}]"
        self.logger.info(string)
        if self.empty(technicals): return
        return technicals

    def download(self, *args, **kwargs):
        technicals = self.page(*args, **kwargs)
        assert isinstance(technicals, pd.DataFrame)
        return technicals

    @property
    def technical(self): return self.__technical
    @property
    def query(self): return self.__query
    @property
    def page(self): return self.__page

class YahooHistoryDownloader(YahooTechnicalDownloader):
    def __init__(self, *args, **kwargs):
        parameters = dict(technical=Variables.Technicals.HISTORY, query=Querys.Symbol)
        super().__init__(*args, **parameters, **kwargs)



