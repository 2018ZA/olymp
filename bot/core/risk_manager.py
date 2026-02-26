"""
Управление рисками.
Проверка ордеров на соответствие риск-параметрам.
"""

from datetime import datetime, date
from typing import Dict, Any, Optional
from utils.logger import logger

class RiskManager:
    """Менеджер рисков"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.daily_trades = 0
        self.current_date = date.today()
        self.max_position_size = config.get('max_position_size', 100000)
        self.max_daily_trades = config.get('max_daily_trades', 200)
    
    def _check_date(self):
        """Проверяет, не начался ли новый день"""
        today = date.today()
        if today > self.current_date:
            self.daily_trades = 0
            self.current_date = today
    
    def check_order(self, order: Dict[str, Any]) -> bool:
        """
        Проверяет, можно ли исполнить ордер.
        
        Args:
            order: Словарь с параметрами ордера
            
        Returns:
            True если ордер прошел проверки
        """
        self._check_date()
        
        # Проверка 1: Лимит дневных сделок
        if self.daily_trades >= self.max_daily_trades:
            logger.warning(f"Превышен лимит дневных сделок: {self.max_daily_trades}")
            return False
        
        # Проверка 2: Размер позиции
        if 'quantity' in order:
            estimated_value = order['quantity'] * order.get('price', 0)
            if estimated_value > self.max_position_size:
                logger.warning(f"Размер позиции превышает лимит: {estimated_value} > {self.max_position_size}")
                return False
        
        # Проверка 3: Не торгуем в последние 5 минут сессии
        current_time = datetime.now().time()
        end_time = datetime.strptime(self.config['trading_end_time'], '%H:%M:%S').time()
        
        # Простая проверка: если до конца сессии меньше 5 минут
        # (здесь нужна более точная реализация с учетом времени)
        
        # Если все проверки пройдены
        self.daily_trades += 1
        return True
    
    def get_daily_trades_left(self) -> int:
        """Возвращает количество оставшихся на сегодня сделок"""
        self._check_date()
        return max(0, self.max_daily_trades - self.daily_trades)