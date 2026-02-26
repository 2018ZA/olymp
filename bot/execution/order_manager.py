"""
Управление отправкой ордеров на биржу.
"""

import requests
from datetime import datetime
from typing import Dict, Any, Optional, List
from config.venue_config import ARENAGO_CONFIG
from utils.logger import logger

class OrderManager:
    """Менеджер для отправки ордеров на arenago.ru"""
    
    def __init__(self):
        self.base_url = ARENAGO_CONFIG['url']
        self.token = ARENAGO_CONFIG['token']
        self.bot_name = ARENAGO_CONFIG['bot']
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        # Статистика ордеров
        self.orders_sent = 0
        self.daily_orders = 0
        self.last_order_time = None
        
    def send_order(self, order: Dict[str, Any]) -> bool:
        """
        Отправляет ордер на биржу.
        
        Args:
            order: Словарь с параметрами ордера
            
        Returns:
            True если ордер успешно отправлен
        """
        try:
            # Проверяем лимит дневных ордеров
            if self.daily_orders >= 200:
                logger.warning("Достигнут лимит дневных ордеров (200)")
                return False
            
            # Формируем запрос для arenago.ru
            if 'pair_instrument' in order:
                # Парный ордер - отправляем два ордера
                success1 = self._send_single_order({
                    'instrument': order['instrument'],
                    'side': self._get_side(order['signal'], is_main=True),
                    'quantity': order['quantity']
                })
                
                success2 = self._send_single_order({
                    'instrument': order['pair_instrument'],
                    'side': self._get_side(order['signal'], is_main=False),
                    'quantity': order['pair_quantity']
                })
                
                return success1 and success2
            else:
                # Обычный ордер
                return self._send_single_order({
                    'instrument': order['instrument'],
                    'side': 'buy' if order['signal'] == 'buy' else 'sell',
                    'quantity': order['quantity']
                })
                
        except Exception as e:
            logger.error(f"Ошибка отправки ордера: {e}")
            return False
    
    def _send_single_order(self, order_data: Dict) -> bool:
        """Отправляет один ордер"""
        try:
            payload = {
                'bot': self.bot_name,
                'ticker': order_data['instrument'],
                'side': order_data['side'].upper(),  # 'BUY' или 'SELL'
                'lots': order_data['quantity']
            }
            
            response = requests.post(
                f"{self.base_url}/api/orders",
                headers=self.headers,
                json=payload
            )
            
            if response.status_code == 200:
                self.orders_sent += 1
                self.daily_orders += 1
                self.last_order_time = datetime.now()
                
                logger.info(f"Ордер отправлен: {order_data['instrument']} "
                           f"{order_data['side']} {order_data['quantity']} лотов")
                return True
            else:
                logger.error(f"Ошибка API: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка отправки ордера: {e}")
            return False
    
    def _get_side(self, signal: str, is_main: bool) -> str:
        """Определяет сторону ордера для парного трейдинга"""
        if signal == 'buy_pair':
            return 'buy' if is_main else 'sell'
        elif signal == 'sell_pair':
            return 'sell' if is_main else 'buy'
        elif signal == 'close_pair':
            return 'close'
        return signal
    
    def close_all_positions(self, positions: List[Dict]) -> bool:
        """
        Закрывает все открытые позиции.
        
        Args:
            positions: Список открытых позиций
            
        Returns:
            True если все позиции успешно закрыты
        """
        success = True
        for position in positions:
            order = {
                'instrument': position['instrument'],
                'side': 'sell' if position['quantity'] > 0 else 'buy',
                'quantity': abs(position['quantity'])
            }
            if not self._send_single_order(order):
                success = False
        return success
    
    def reset_daily_counter(self):
        """Сбрасывает счетчик дневных ордеров"""
        self.daily_orders = 0