# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Yahoo History Downloader
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import xarray as xr
import pandas as pd
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJ = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJ, os.pardir))
if ROOT not in sys.path:
    sys.path.append(ROOT)
SAVE = os.path.join(ROOT, "Library", "repository")
CHROME = os.path.join(ROOT, "Library", "resources", "chromedriver.exe")

from webscraping.webdrivers import WebDriver
from support.synchronize import Queue, Consumer
from finance.securities import DateRange, HistorySaver

from history import YahooHistoryDownloader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 20)
pd.set_option("display.max_columns", 25)


class YahooDriver(WebDriver, browser="chrome", file=CHROME, delay=10):
    pass


def main(tickers, *args, history, **kwargs):
    source = Queue(tickers, size=None, name="YahooTickerQueue")
    with YahooDriver(timeout=60, name="YahooDriver") as driver:
        downloader = YahooHistoryDownloader(source=driver, name="YahooHistoryDownloader")
        saver = HistorySaver(repository=SAVE, name="YahooHistorySaver")
        pipeline = downloader + saver
        consumer = Consumer(pipeline, source=source, name="YahooDownloader")
        consumer.setup(history=history)
        consumer.start()
        consumer.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    sysTickers = ["TSLA", "AAPL", "IWM", "AMZN", "NVDA", "AMD", "AMC", "SPY", "QQQ", "MSFT", "BAC", "BABA", "GOOGL", "META", "ZIM", "XOM", "INTC", "OXY", "CSCO", "COIN", "NIO"]
    sysHistory = DateRange([(Datetime.today() - Timedelta(days=1)).date(), (Datetime.today() - Timedelta(days=600)).date()])
    main(sysTickers, history=sysHistory)

