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

from finance.historicals import HistorySaver, HistoryFile
from finance.variables import DateRange
from webscraping.webdrivers import WebDriver, WebBrowser
from support.files import Archive, FileTiming, FileTyping
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


class YahooDriver(WebDriver, browser=WebBrowser.CHROME, executable=CHROME, delay=10):
    pass


def history(reader, archive, *args, tickers, dates, **kwargs):
    history_downloader = YahooHistoryDownloader(name="HistoryBarDownloader", feed=reader)
    history_saver = HistorySaver(name="HistoryBarSaver", destination=archive, mode="a")
    history_pipeline = history_downloader + history_saver
    history_thread = SideThread(history_pipeline, name="HistoryBarThread")
    history_thread.setup(tickers=tickers, dates=dates)
    return history_thread

def main(*args, **kwargs):
    history_file = HistoryFile(name="HistoryBarFile", typing=FileTyping.CSV, timing=FileTiming.EAGER, duplicates=False)
    history_archive = Archive(name="HistoryArchive", repository=HISTORY, save=[history_file])
    with YahooDriver(name="HistoryReader") as history_reader:
        history_thread = history(history_reader, history_archive, *args, **kwargs)
        history_thread.start()
        history_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    logging.getLogger("seleniumwire").setLevel(logging.ERROR)
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:2]
    sysDates = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() - Timedelta(weeks=60)).date()])
    main(tickers=sysTickers, dates=sysDates)



