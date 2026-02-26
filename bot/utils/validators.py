"""
Функции для валидации данных.
"""

import pandas as pd
import numpy as np
from typing import Optional

def validate_market_data(df: pd.DataFrame) -> bool:
    """
    Проверяет качество рыночных данных.
    
    Args:
        df: DataFrame с рыночными данными
        
    Returns:
        True если данные валидны
    """
    if df.empty:
        return False
    
    # Проверяем наличие необходимых колонок
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_columns):
        return False
    
    # Проверяем на пропуски
    if df[required_columns].isnull().any().any():
        return False
    
    # Проверяем на аномальные значения
    if (df['high'] < df['low']).any():
        return False
    
    if (df['close'] <= 0).any():
        return False
    
    # Проверяем на слишком большие скачки (>20% за одну свечу)
    pct_changes = df['close'].pct_change().abs()
    if (pct_changes > 0.2).any():
        # Это может быть нормально при сильных движениях, только логируем
        pass
    
    return True

def validate_order(order: dict) -> bool:
    """
    Проверяет корректность ордера.
    
    Args:
        order: Словарь с параметрами ордера
        
    Returns:
        True если ордер корректен
    """
    required_fields = ['instrument', 'quantity', 'signal']
    
    for field in required_fields:
        if field not in order:
            return False
    
    if order['quantity'] <= 0:
        return False
    
    if order['signal'] not in ['buy', 'sell', 'buy_pair', 'sell_pair', 'close_pair']:
        return False
    
    return True

def validate_strategy_params(params: dict, required_params: list) -> bool:
    """
    Проверяет наличие необходимых параметров стратегии.
    
    Args:
        params: Словарь параметров
        required_params: Список обязательных параметров
        
    Returns:
        True если все обязательные параметры присутствуют
    """
    return all(param in params for param in required_params)