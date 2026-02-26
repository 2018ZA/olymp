"""
Базовый класс для всех торговых стратегий.
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any
from datetime import datetime
from utils.logger import logger

class BaseStrategy(ABC):
    """Абстрактный базовый класс для всех стратегий"""
    
    def __init__(self, instrument: str, params: Dict[str, Any], quantity: int = 1):
        """
        Args:
            instrument: Тикер инструмента
            params: Параметры стратегии
            quantity: Количество лотов для торговли
        """
        self.instrument = instrument
        self.params = params
        self.quantity = quantity
        self.name = f"{self.__class__.__name__}_{instrument}"
        
        # Данные
        self.data = pd.DataFrame()
        self.last_signal = None
        self.current_position = 0  # Текущая позиция (положительная - лонг)
        
        # Управление рисками
        self.stop_loss_price = None
        self.take_profit_price = None
        self.entry_price = None
        
        # Статистика
        self.trades_count = 0
        self.last_trade_time = None
        
        logger.info(f"Создана стратегия {self.name}")
    
    @abstractmethod
    def generate_signal(self) -> Optional[str]:
        """
        Генерирует торговый сигнал.
        
        Returns:
            'buy', 'sell' или None
        """
        pass
    
    def on_data(self, data: pd.DataFrame) -> None:
        """
        Обрабатывает новые данные.
        
        Args:
            data: DataFrame с новыми данными
        """
        self.data = data
        self.update_risk_management()
    
    def set_initial_data(self, data: pd.DataFrame) -> None:
        """Устанавливает начальные исторические данные"""
        self.data = data
    
    def has_order_signal(self) -> bool:
        """Проверяет, есть ли сигнал на ордер"""
        signal = self.generate_signal()
        if signal and signal != self.last_signal:
            self.last_signal = signal
            return True
        return False
    
    def get_order(self) -> Dict[str, Any]:
        """
        Создает ордер на основе текущего сигнала.
        
        Returns:
            Словарь с параметрами ордера
        """
        if not self.last_signal or self.data.empty:
            return {}
        
        current_price = self.data['close'].iloc[-1]
        
        order = {
            'instrument': self.instrument,
            'quantity': self.quantity,
            'price': current_price,
            'signal': self.last_signal,
            'timestamp': datetime.now(),
            'strategy': self.name
        }
        
        # Обновляем позицию
        if self.last_signal == 'buy':
            self.current_position += self.quantity
            self.entry_price = current_price
        elif self.last_signal == 'sell':
            self.current_position -= self.quantity
            self.entry_price = current_price
        
        self.trades_count += 1
        self.last_trade_time = datetime.now()
        
        return order
    
    def update_risk_management(self) -> None:
        """Обновляет уровни стоп-лосс и тейк-профит"""
        if self.current_position != 0 and self.entry_price is not None:
            current_price = self.data['close'].iloc[-1]
            
            # Рассчитываем ATR для динамических уровней
            from indicators.technical import calculate_atr
            atr = calculate_atr(
                self.data['high'].values,
                self.data['low'].values,
                self.data['close'].values
            )
            
            if atr > 0:
                if 'stop_loss_atr_multiple' in self.params:
                    atr_multiple = self.params['stop_loss_atr_multiple']
                    
                    if self.current_position > 0:  # Лонг
                        self.stop_loss_price = self.entry_price - (atr * atr_multiple)
                    else:  # Шорт
                        self.stop_loss_price = self.entry_price + (atr * atr_multiple)
    
    def check_stop_loss(self) -> bool:
        """Проверяет, сработал ли стоп-лосс"""
        if self.stop_loss_price is None or self.data.empty:
            return False
        
        current_price = self.data['close'].iloc[-1]
        
        if self.current_position > 0:  # Лонг
            return current_price <= self.stop_loss_price
        elif self.current_position < 0:  # Шорт
            return current_price >= self.stop_loss_price
        
        return False
    
    def reset(self) -> None:
        """Сбрасывает состояние стратегии"""
        self.last_signal = None
        self.current_position = 0
        self.stop_loss_price = None
        self.take_profit_price = None
        self.entry_price = None