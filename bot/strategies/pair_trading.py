"""
Стратегия парного трейдинга (статистический арбитраж).
"""

import datetime

import numpy as np
import pandas as pd
import statsmodels.api as sm
from typing import Optional, Dict, Any, Tuple
from .base_strategy import BaseStrategy
from utils.logger import logger

class PairTradingStrategy(BaseStrategy):
    """
    Стратегия парного трейдинга.
    
    Торгует на отклонении спреда между двумя инструментами от среднего.
    """
    
    def __init__(self, instrument: str, params: Dict[str, Any], quantity: int = 1):
        """
        Args:
            instrument: Основной инструмент (первый в паре)
            params: Параметры стратегии
                - pair_instrument: Второй инструмент в паре
                - lookback_period: Период для расчета спреда
                - entry_z: Порог входа (в сигмах)
                - exit_z: Порог выхода (в сигмах)
                - hedge_ratio_update: Как часто пересчитывать коэффициент хеджа
            quantity: Количество лотов для основного инструмента
        """
        super().__init__(instrument, params, quantity)
        
        self.pair_instrument = params.get('pair_instrument')
        if not self.pair_instrument:
            raise ValueError("Не указан парный инструмент")
        
        self.lookback_period = params.get('lookback_period', 100)
        self.entry_z = params.get('entry_z', 2.0)
        self.exit_z = params.get('exit_z', 0.5)
        self.hedge_ratio_update = params.get('hedge_ratio_update', 50)
        
        # Данные для парного инструмента
        self.pair_data = pd.DataFrame()
        
        # Коэффициенты
        self.hedge_ratio = 1.0
        self.spread_mean = 0.0
        self.spread_std = 1.0
        self.update_counter = 0
        
        # Текущая позиция по парному инструменту
        self.pair_position = 0
        
        logger.info(f"Pair Trading инициализирован: {instrument} / {self.pair_instrument}")
    
    def set_pair_data(self, data: pd.DataFrame) -> None:
        """Устанавливает данные для парного инструмента"""
        self.pair_data = data
    
    def calculate_hedge_ratio(self) -> float:
        """
        Рассчитывает коэффициент хеджирования через линейную регрессию.
        
        Returns:
            Коэффициент хеджирования (beta)
        """
        if self.data.empty or self.pair_data.empty:
            return self.hedge_ratio
        
        # Берем последние lookback_period значений
        min_len = min(len(self.data), len(self.pair_data), self.lookback_period)
        
        y = self.data['close'].values[-min_len:]
        x = self.pair_data['close'].values[-min_len:]
        
        # Добавляем константу для регрессии
        x_with_const = sm.add_constant(x)
        
        try:
            model = sm.OLS(y, x_with_const).fit()
            beta = model.params[1]  # Коэффициент при x
            return beta
        except:
            return self.hedge_ratio
    
    def calculate_spread(self) -> Tuple[float, float, float]:
        """
        Рассчитывает текущий спред и его статистики.
        
        Returns:
            Кортеж (текущий спред, среднее спреда, стандартное отклонение)
        """
        if self.data.empty or self.pair_data.empty:
            return 0.0, 0.0, 1.0
        
        min_len = min(len(self.data), len(self.pair_data))
        
        # Рассчитываем историю спреда
        prices1 = self.data['close'].values[-min_len:]
        prices2 = self.pair_data['close'].values[-min_len:]
        
        spread_history = prices1 - self.hedge_ratio * prices2
        
        # Текущий спред
        current_spread = spread_history[-1]
        
        # Статистики за lookback_period
        lookback = min(min_len, self.lookback_period)
        spread_mean = np.mean(spread_history[-lookback:])
        spread_std = np.std(spread_history[-lookback:])
        
        if spread_std == 0:
            spread_std = 1.0
        
        return current_spread, spread_mean, spread_std
    
    def generate_signal(self) -> Optional[str]:
        """Генерирует сигнал на основе Z-score спреда"""
        if self.data.empty or self.pair_data.empty:
            return None
        
        # Обновляем коэффициент хеджирования
        self.update_counter += 1
        if self.update_counter >= self.hedge_ratio_update:
            self.hedge_ratio = self.calculate_hedge_ratio()
            self.update_counter = 0
        
        # Рассчитываем спред
        current_spread, spread_mean, spread_std = self.calculate_spread()
        
        # Сохраняем для использования в get_order
        self.spread_mean = spread_mean
        self.spread_std = spread_std
        
        # Рассчитываем Z-score
        z_score = (current_spread - spread_mean) / spread_std
        
        # Проверяем условия входа
        if abs(z_score) > self.entry_z and self.current_position == 0:
            if z_score > 0:  # Спред слишком высокий
                # Продаем основной, покупаем парный
                logger.info(f"{self.name}: Z-score={z_score:.2f} > {self.entry_z} -> SELL {self.instrument}, BUY {self.pair_instrument}")
                return 'sell_pair'
            else:  # Спред слишком низкий
                # Покупаем основной, продаем парный
                logger.info(f"{self.name}: Z-score={z_score:.2f} < -{self.entry_z} -> BUY {self.instrument}, SELL {self.pair_instrument}")
                return 'buy_pair'
        
        # Проверяем условия выхода
        elif abs(z_score) < self.exit_z and self.current_position != 0:
            logger.info(f"{self.name}: Z-score={z_score:.2f} < {self.exit_z} -> CLOSE")
            return 'close_pair'
        
        return None
    
    def get_order(self) -> Dict[str, Any]:
        """
        Создает ордера для парной стратегии.
        Возвращает словарь с ордерами для обоих инструментов.
        """
        if not self.last_signal or self.data.empty or self.pair_data.empty:
            return {}
        
        current_price = self.data['close'].iloc[-1]
        pair_price = self.pair_data['close'].iloc[-1]
        
        orders = {
            'instrument': self.instrument,
            'pair_instrument': self.pair_instrument,
            'quantity': self.quantity,
            'pair_quantity': int(self.quantity * self.hedge_ratio),  # Количество для парного инструмента
            'signal': self.last_signal,
            'timestamp': datetime.now(),
            'strategy': self.name
        }
        
        # Обновляем позиции
        if self.last_signal == 'buy_pair':
            self.current_position = self.quantity
            self.pair_position = -int(self.quantity * self.hedge_ratio)
            self.entry_price = current_price
        elif self.last_signal == 'sell_pair':
            self.current_position = -self.quantity
            self.pair_position = int(self.quantity * self.hedge_ratio)
            self.entry_price = current_price
        elif self.last_signal == 'close_pair':
            self.current_position = 0
            self.pair_position = 0
            self.entry_price = None
        
        self.trades_count += 1
        self.last_trade_time = datetime.now()
        
        return orders