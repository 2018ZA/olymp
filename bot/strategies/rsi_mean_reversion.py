"""
Стратегия на возврате к среднему на основе RSI.
"""

from typing import Optional
import pandas as pd
from .base_strategy import BaseStrategy
from indicators.technical import calculate_rsi
from utils.logger import logger

class RSIMeanReversion(BaseStrategy):
    """
    Стратегия, торгующая на перекупленности/перепроданности по RSI.
    
    Сигналы:
        BUY: когда RSI опускается ниже уровня перепроданности
        SELL: когда RSI поднимается выше уровня перекупленности
    """
    
    def __init__(self, instrument: str, params: dict, quantity: int = 1):
        """
        Args:
            instrument: Тикер инструмента
            params: Параметры стратегии
                - rsi_period: период RSI (по умолчанию 14)
                - oversold: уровень перепроданности (по умолчанию 30)
                - overbought: уровень перекупленности (по умолчанию 70)
            quantity: Количество лотов
        """
        super().__init__(instrument, params, quantity)
        
        self.rsi_period = params.get('rsi_period', 14)
        self.oversold_level = params.get('oversold', 30)
        self.overbought_level = params.get('overbought', 70)
        
        logger.info(f"RSI Mean Reversion инициализирована: период={self.rsi_period}, "
                   f"перепроданность={self.oversold_level}, перекупленность={self.overbought_level}")
    
    def generate_signal(self) -> Optional[str]:
        """Генерирует сигнал на основе RSI"""
        if self.data.empty or len(self.data) < self.rsi_period + 1:
            return None
        
        # Рассчитываем текущее значение RSI
        current_rsi = calculate_rsi(self.data['close'].values, self.rsi_period)
        
        # Проверяем условия
        if current_rsi < self.oversold_level:
            # Проверяем, не в лонге ли мы уже
            if self.current_position <= 0:
                logger.info(f"{self.name}: RSI={current_rsi:.2f} < {self.oversold_level} -> BUY")
                return 'buy'
        
        elif current_rsi > self.overbought_level:
            # Проверяем, не в шорте ли мы уже
            if self.current_position >= 0:
                logger.info(f"{self.name}: RSI={current_rsi:.2f} > {self.overbought_level} -> SELL")
                return 'sell'
        
        return None