"""
Инициализация модуля стратегий.
"""

from .base_strategy import BaseStrategy
from .sma_strategy import SMACrossover
from .rsi_mean_reversion import RSIMeanReversion
from .pair_trading import PairTradingStrategy

__all__ = [
    'BaseStrategy',
    'SMACrossover',
    'RSIMeanReversion',
    'PairTradingStrategy'
]