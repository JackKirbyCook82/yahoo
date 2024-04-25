# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   Yahoo Trading Platform Technicals
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import xarray as xr
import pandas as pd
import PySimpleGUI as gui

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
REPOSITORY = os.path.join(ROOT, "Library", "repository")
BARS = os.path.join(REPOSITORY, "history", "bars")
STATISTICS = os.path.join(REPOSITORY, "history", "statistics")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from finance.technicals import TechnicalCalculator, BarFile, StatisticFile
from support.files import Loader, Saver, Directory, Timing, Typing
from support.synchronize import SideThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


gui.theme("DarkGrey11")
warnings.filterwarnings("ignore")
xr.set_options(**{"display_width": 200})
xr.set_options(**{"display_max_rows": 35})
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", 20)
pd.set_option("display.max_columns", 25)


def technical(source, destination, *args, parameters, **kwargs):
    ticker_directory = Directory("ticker", lambda folder: str(folder).upper(), repository=BARS)
    bar_loader = Loader(name="BarLoader", source=source, directory=ticker_directory)
    technical_calculator = TechnicalCalculator("TechnicalCalculator")
    technical_saver = Saver(name="TechnicalSaver", destination=destination, query="ticker")
    technical_pipeline = bar_loader + technical_calculator + technical_saver
    technical_thread = SideThread(technical_pipeline, name="TechnicalThread")
    technical_thread.setup(**parameters)
    return technical_thread


def main(*args, **kwargs):
    ticker_query = lambda ticker: str(ticker).upper()
    bar_file = BarFile(name="BarFile", repository=BARS, query=ticker_query, typing=Typing.CSV, timing=Timing.EAGER, duplicates=False)
    statistic_file = StatisticFile(name="StatisticFile", repository=STATISTICS, query=ticker_query, typing=Typing.CSV, timing=Timing.EAGER, duplicates=False)
    technical_thread = technical({bar_file: "r"}, {statistic_file: "w"}, *args, **kwargs)
    technical_thread.start()
    technical_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    main(parameters={"period": 252})



