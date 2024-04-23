# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 2024
@name:   Yahoo Trading Platform Downloader
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
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
REPOSITORY = os.path.join(ROOT, "Library", "repository")
HISTORY = os.path.join(REPOSITORY, "history")
TICKERS = os.path.join(ROOT, "AlgoTrading", "tickers.txt")
CHROME = os.path.join(ROOT, "Library", "resources", "chromedriver.exe")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.variables import DateRange
from finance.technicals import HistoryFile
from webscraping.webdrivers import WebDriver, WebBrowser
from support.files import Saver, FileTiming, FileTyping
from support.queues import Schedule, Queues
from support.synchronize import SideThread

from history import YahooHistoryDownloader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2024, Jack Kirby Cook"
__license__ = "MIT License"


warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", 25)


class YahooDriver(WebDriver, browser=WebBrowser.CHROME, executable=CHROME, delay=10): pass
class TickerQueue(Queues.FIFO, variable="ticker"): pass


def history(tickers, reader, historicals, *args, dates, parameters, **kwargs):
    history_folder = lambda contents: str(contents["contract"].tostring(delimiter="_"))
    history_schedule = Schedule(name="HistorySchedule", source=tickers)
    history_downloader = YahooHistoryDownloader(name="HistoryDownloader", feed=reader)
    history_saver = Saver(name="HistorySaver", destination=historicals, folder=history_folder, mode="a")
    history_pipeline = history_schedule + history_downloader + history_saver
    history_thread = SideThread(history_pipeline, name="HistoryThread")
    history_thread.setup(dates=dates, **parameters)
    return history_thread


def main(*args, tickers, **kwargs):
    ticker_queue = TickerQueue(name="TickerQueue", contents=list(tickers), capacity=None)
    history_file = HistoryFile(name="HistoryFile", repository=HISTORY, typing=FileTyping.CSV, timing=FileTiming.EAGER, duplicates=False)
    with YahooDriver(name="HistoryReader") as history_reader:
        history_thread = history(ticker_queue, history_reader, history_file, *args, **kwargs)
        history_thread.start()
        history_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:2]
    sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=60)).date()])
    main(tickers=sysTickers, dates=sysDates, parameters={})
