# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo History Objects
@author: Jack Kirby Cook

"""

import regex as re
import numpy as np
import pandas as pd
from datetime import datetime as Datetime

from finance.variables import Querys
from webscraping.webpages import WebELMTPage
from webscraping.webdatas import WebHTML
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["YahooBarsDownloader"]
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


bars_parsers = {key: np.float32 for keys in "open high low close price" for key in str(keys).split(" ")} | {"date": "datetime64[D]", "volume": np.int64}
ticker_parser = lambda string: re.search(r"\[[A-Z]+\]", string)


class YahooBarsURL(WebURL, domain="https://finance.yahoo.com", path=["quote"], parms={"frequency": "1d", "includeAdjustedClose": "true"}):
    @staticmethod
    def path(*args, ticker, **kwargs): return [str(ticker), "history"]
    @staticmethod
    def parms(*args, dates, **kwargs):
        start = Datetime.combine(dates.minimum, Datetime.min.time()).timestamp()
        stop = Datetime.combine(dates.maximum, Datetime.min.time()).timestamp()
        return {"period1": int(start), "period2": int(stop)}


class YahooBarsData(WebHTML, locator="//article[contains(@class, 'gridLayout')]", multiple=False, optional=False):
    class Ticker(WebHTML.Text, locator="//section[contains(@class, 'container')]/h1", key="ticker", parser=ticker_parser): pass
    class Table(WebHTML.Table, locator="//table[contains(@class, 'table')]", key="table"):
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
        dataframe = dataframe.astype(bars_parsers)
        dataframe["ticker"] = contents["ticker"]
        return dataframe


class YahooBarsPage(WebELMTPage, url=YahooBarsURL, variable=YahooBarsData):
    pass


class YahooBarsDownloader(Sizing, Emptying, Logging, title="Downloaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = YahooBarsPage(*args, **kwargs)

    def execute(self, symbols, *args, **kwargs):
        assert isinstance(symbols, (list, Querys.Symbol))
        assert all([isinstance(symbol, Querys.Symbol) for symbol in symbols]) if isinstance(symbols, list) else True
        symbols = list(symbols) if isinstance(symbols, list) else [symbols]
        for symbol in list(symbols):
            bars = self.download(symbol, *args, **kwargs)
            size = self.size(bars)
            self.console(f"{str(symbol)}[{int(size):.0f}]")
            if self.empty(bars): continue
            yield bars

    def download(self, symbol, *args, dates, **kwargs):
        parameters = dict(ticker=symbol.ticker, dates=dates)
        bars = self.page(*args, **parameters, **kwargs)
        assert isinstance(bars, dict)
        return bars

    @property
    def page(self): return self.__page



