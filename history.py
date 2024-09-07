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
from collections import OrderedDict as ODict

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


class YahooHistoryURL(WebURL):
    def domain(cls, *args, **kwargs): return "https://finance.yahoo.com"
    def path(cls, *args, ticker, **kwargs): return f"/quote/{str(ticker)}/history"
    def parms(cls, *args, dates, **kwargs):
        start = Datetime.combine(dates.minimum, Datetime.min.time()).timestamp()
        stop = Datetime.combine(dates.maximum, Datetime.min.time()).timestamp()
        return {"period1": start, "period2": stop, "frequency": "1d", "includeAdjustedClose": "true"}


class YahooHistoryData(WebHTML.Table, locator=r"//table", key="history", parser=Parsers.history): pass
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
        function = lambda parsers: lambda columns: {column: parser(columns[column]) for column, parser in parsers.items()}
        prices = dataframe.apply(function(Parsers.prices), axis=1, result_type="expand")
        volumes = dataframe.apply(function(Parsers.volumes), axis=1, result_type="expand")
        dates = dataframe.apply(function(Parsers.dates), axis=1, result_type="expand")
        dataframe = pd.concat([dates, prices, volumes], axis=1)
        dataframe = dataframe.sort_values("date", axis=0, ascending=True, inplace=False)
        dataframe["ticker"] = str(ticker).upper()
        return dataframe


class YahooHistoryDownloader(Processor, title="Downloaded"):
    def __init__(self, *args, name=None, **kwargs):
        super().__init__(*args, name=name, **kwargs)
        pages = {Variables.Technicals.BARS: YahooHistoryPage}
        self.__pages = {variable: page(*args, **kwargs) for variable, page in pages.items()}

    def processor(self, contents, *args, dates, **kwargs):
        ticker = contents[Variables.Querys.SYMBOL].ticker
        assert isinstance(ticker, str)
        parameters = dict(ticker=ticker, dates=dates)
        technicals = ODict(list(self.download(*args, **parameters, **kwargs)))
        if not bool(technicals): return
        yield contents | technicals

    def download(self, *args, ticker, dates, **kwargs):
        for variable, page in self.pages.items():
            technical = page(*args, ticker=ticker, dates=dates, **kwargs)
            if bool(technical.empty): continue
            yield variable, technical

    @property
    def pages(self): return self.__pages



