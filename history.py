# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo History Objects
@author: Jack Kirby Cook

"""

import logging
import pandas as pd
from datetime import datetime as Datetime

from webscraping.webpages import WebELMTPage
from webscraping.webdatas import WebHTML
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"
__logger__ = logging.getLogger(__name__)


class YahooBarsURL(WebURL, domain="https://finance.yahoo.com", path=["quote"], parms={"frequency": "1d", "includeAdjustedClose": "true"}):
    @staticmethod
    def path(*args, ticker, **kwargs): return [str(ticker), "history"]
    @staticmethod
    def parms(*args, dates, **kwargs):
        start = Datetime.combine(dates.minimum, Datetime.min.time()).timestamp()
        stop = Datetime.combine(dates.maximum, Datetime.min.time()).timestamp()
        return {"period1": int(start), "period2": int(stop)}


class YahooBarsData(WebHTML, locator="//article[contains(@class, 'gridLayout')]", multiple=False, optional=False):
    class Ticker(WebHTML.Text, locator="//section[contains(@class, 'container')]/h1", key="ticker", parser=PARSER): pass
    class Table(WebHTML.Table, locator="//table[contains(@class, 'table')]", key="table", header=PARSERS):
        @staticmethod
        def parse(dataframe, *args, **kwargs):
            assert isinstance(dataframe, pd.DataFrame)
            dataframe.columns = [str(column).lower() for column in dataframe.columns]
            dataframe = dataframe.rename(columns={"adj close": "adjusted"}, inplace=False)
            dividend = ~dataframe["open"].apply(str).str.contains("Dividend")
            split = ~dataframe["open"].apply(str).str.contains("Split")
            dataframe = dataframe[dividend & split]
            return dataframe

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        assert isinstance(contents, dict)
        dataframe = contents["table"].sort_values("date", axis=0, ascending=True, inplace=False)
        dataframe["ticker"] = contents["ticker"]
        return dataframe


class YahooBarsPage(WebELMTPage, url=YahooBarsURL, data={(STOCK, BARS): YahooBarsData}):
    pass


class YahooBarsDownloader(Sizing, Emptying):
    def __init__(self, *args, **kwargs):
        self.page = YahooBarsPage(*args, **kwargs)

    def execute(self, *args, symbols, dates, **kwargs):
        assert isinstance(symbols, list)
        for symbol in iter(symbols):
            parameters = dict(ticker=symbol.ticker, dates=dates)
            contents = self.page(*args, **parameters, **kwargs)
            assert isinstance(contents, dict)
            for dataset, content in contents.items():
                string = "|".join(list(map(str, dataset)))
                size = self.size(contents)
                string = f"Downloaded: {repr(self)}|{str(string)}|{str(symbol)}[{int(size):.0f}]"
                __logger__.info(string)
            if self.empty(contents): continue
            yield contents





