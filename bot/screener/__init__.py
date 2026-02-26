"""
Модуль для скрининга и оценки перспективности акций.
Позволяет получать топ акций для покупки и подбирать акции под стратегии.
"""

from .stock_screener import StockScreener
from .strategy_matcher import StrategyMatcher
from .ranker import Ranker
from .reporters import ConsoleReporter, HTMLReporter

__all__ = [
    'StockScreener',
    'StrategyMatcher', 
    'Ranker',
    'ConsoleReporter',
    'HTMLReporter'
]