# indicators/technical.py
"""
Модуль с техническими индикаторами для анализа акций.
Все индикаторы работают с pandas Series или numpy массивами.
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional, Dict, Union
import logging

logger = logging.getLogger(__name__)

def ensure_numpy_array(data):
    """
    Преобразует входные данные в numpy массив.
    
    Args:
        data: pandas Series, list или numpy array
        
    Returns:
        numpy array
    """
    if isinstance(data, pd.Series):
        return data.values.astype(float)
    elif isinstance(data, list):
        return np.array(data, dtype=float)
    elif isinstance(data, np.ndarray):
        return data.astype(float)
    else:
        try:
            return np.array(data, dtype=float)
        except:
            logger.error(f"Не удалось преобразовать данные в numpy array: {type(data)}")
            return np.array([])

def calculate_sma(data: Union[pd.Series, np.ndarray, list], period: int = 20) -> np.ndarray:
    """
    Рассчитывает простое скользящее среднее (SMA).
    
    Args:
        data: Входные данные (цены закрытия)
        period: Период SMA
        
    Returns:
        Массив со значениями SMA
    """
    data = ensure_numpy_array(data)
    
    if len(data) < period:
        logger.warning(f"Недостаточно данных для SMA: {len(data)} < {period}")
        return np.array([])
    
    sma = np.zeros_like(data)
    sma[:period-1] = np.nan
    
    for i in range(period-1, len(data)):
        sma[i] = np.mean(data[i-period+1:i+1])
    
    return sma

def calculate_ema(data: Union[pd.Series, np.ndarray, list], period: int = 20) -> np.ndarray:
    """
    Рассчитывает экспоненциальное скользящее среднее (EMA).
    
    Args:
        data: Входные данные (цены закрытия)
        period: Период EMA
        
    Returns:
        Массив со значениями EMA
    """
    data = ensure_numpy_array(data)
    
    if len(data) < period:
        logger.warning(f"Недостаточно данных для EMA: {len(data)} < {period}")
        return np.array([])
    
    ema = np.zeros_like(data)
    ema[:period-1] = np.nan
    
    # Начальное значение - SMA первых period элементов
    ema[period-1] = np.mean(data[:period])
    
    multiplier = 2 / (period + 1)
    
    for i in range(period, len(data)):
        ema[i] = (data[i] - ema[i-1]) * multiplier + ema[i-1]
    
    return ema

def calculate_rsi(data: Union[pd.Series, np.ndarray, list], period: int = 14) -> np.ndarray:
    """
    Рассчитывает Relative Strength Index (RSI).
    
    Args:
        data: Входные данные (цены закрытия)
        period: Период RSI
        
    Returns:
        Массив со значениями RSI
    """
    data = ensure_numpy_array(data)
    
    if len(data) <= period:
        logger.warning(f"Недостаточно данных для RSI: {len(data)} <= {period}")
        return np.array([])
    
    # Рассчитываем изменения
    deltas = np.diff(data)
    
    # Разделяем на прибыли и убытки
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    # Средние прибыли и убытки
    avg_gain = np.zeros_like(data)
    avg_loss = np.zeros_like(data)
    
    avg_gain[:period] = np.nan
    avg_loss[:period] = np.nan
    
    # Первое среднее - простое среднее
    avg_gain[period] = np.mean(gains[:period])
    avg_loss[period] = np.mean(losses[:period])
    
    # Последующие - экспоненциальное среднее
    for i in range(period + 1, len(data)):
        avg_gain[i] = (avg_gain[i-1] * (period - 1) + gains[i-1]) / period
        avg_loss[i] = (avg_loss[i-1] * (period - 1) + losses[i-1]) / period
    
    # Рассчитываем RS и RSI
    rs = np.zeros_like(data)
    rs[:period+1] = np.nan
    
    for i in range(period, len(data)):
        if avg_loss[i] != 0:
            rs[i] = avg_gain[i] / avg_loss[i]
        else:
            rs[i] = 100  # Если нет убытков, RSI = 100
    
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def calculate_bollinger_bands(data: Union[pd.Series, np.ndarray, list], 
                             period: int = 20, 
                             std_dev: float = 2.0) -> Dict[str, np.ndarray]:
    """
    Рассчитывает полосы Боллинджера.
    
    Args:
        data: Входные данные (цены закрытия)
        period: Период для SMA
        std_dev: Количество стандартных отклонений
        
    Returns:
        Словарь с массивами 'upper', 'middle', 'lower'
    """
    data = ensure_numpy_array(data)
    
    if len(data) < period:
        logger.warning(f"Недостаточно данных для Bollinger Bands: {len(data)} < {period}")
        return {'upper': np.array([]), 'middle': np.array([]), 'lower': np.array([])}
    
    middle = calculate_sma(data, period)
    std = np.zeros_like(data)
    std[:period-1] = np.nan
    
    for i in range(period-1, len(data)):
        std[i] = np.std(data[i-period+1:i+1])
    
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    
    return {
        'upper': upper,
        'middle': middle,
        'lower': lower
    }

def calculate_atr(high: Union[pd.Series, np.ndarray, list],
                 low: Union[pd.Series, np.ndarray, list],
                 close: Union[pd.Series, np.ndarray, list],
                 period: int = 14) -> np.ndarray:
    """
    Рассчитывает Average True Range (ATR).
    
    Args:
        high: Цены максимума
        low: Цены минимума
        close: Цены закрытия
        period: Период ATR
        
    Returns:
        Массив со значениями ATR
    """
    high = ensure_numpy_array(high)
    low = ensure_numpy_array(low)
    close = ensure_numpy_array(close)
    
    if len(high) <= period or len(low) <= period or len(close) <= period:
        logger.warning(f"Недостаточно данных для ATR")
        return np.array([])
    
    # Рассчитываем True Range
    tr = np.zeros_like(high)
    tr[0] = high[0] - low[0]
    
    for i in range(1, len(high)):
        hl = high[i] - low[i]
        hc = abs(high[i] - close[i-1])
        lc = abs(low[i] - close[i-1])
        tr[i] = max(hl, hc, lc)
    
    # Рассчитываем ATR
    atr = np.zeros_like(high)
    atr[:period] = np.nan
    
    # Первое значение - простое среднее
    atr[period-1] = np.mean(tr[:period])
    
    # Последующие - экспоненциальное среднее
    for i in range(period, len(high)):
        atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
    
    return atr

def calculate_macd(data: Union[pd.Series, np.ndarray, list],
                  fast_period: int = 12,
                  slow_period: int = 26,
                  signal_period: int = 9) -> Dict[str, np.ndarray]:
    """
    Рассчитывает MACD (Moving Average Convergence Divergence).
    
    Args:
        data: Входные данные (цены закрытия)
        fast_period: Период быстрой EMA
        slow_period: Период медленной EMA
        signal_period: Период сигнальной линии
        
    Returns:
        Словарь с массивами 'macd', 'signal', 'histogram'
    """
    data = ensure_numpy_array(data)
    
    if len(data) < slow_period + signal_period:
        logger.warning(f"Недостаточно данных для MACD")
        return {'macd': np.array([]), 'signal': np.array([]), 'histogram': np.array([])}
    
    # Рассчитываем EMA
    ema_fast = calculate_ema(data, fast_period)
    ema_slow = calculate_ema(data, slow_period)
    
    # MACD линия
    macd = np.zeros_like(data)
    macd[:slow_period-1] = np.nan
    
    for i in range(slow_period-1, len(data)):
        if not np.isnan(ema_fast[i]) and not np.isnan(ema_slow[i]):
            macd[i] = ema_fast[i] - ema_slow[i]
        else:
            macd[i] = np.nan
    
    # Сигнальная линия (EMA от MACD)
    signal = np.zeros_like(data)
    signal[:slow_period + signal_period - 2] = np.nan
    
    # Находим первый не-NaN индекс в MACD
    valid_indices = np.where(~np.isnan(macd))[0]
    if len(valid_indices) > signal_period:
        start_idx = valid_indices[0]
        
        # Рассчитываем сигнальную линию
        valid_macd = macd[start_idx:]
        signal_line = calculate_ema(valid_macd, signal_period)
        
        # Вставляем обратно
        signal[start_idx:start_idx + len(signal_line)] = signal_line
    
    # Гистограмма
    histogram = macd - signal
    
    return {
        'macd': macd,
        'signal': signal,
        'histogram': histogram
    }

def calculate_stochastic(high: Union[pd.Series, np.ndarray, list],
                        low: Union[pd.Series, np.ndarray, list],
                        close: Union[pd.Series, np.ndarray, list],
                        k_period: int = 14,
                        d_period: int = 3) -> Dict[str, np.ndarray]:
    """
    Рассчитывает стохастический осциллятор.
    
    Args:
        high: Цены максимума
        low: Цены минимума
        close: Цены закрытия
        k_period: Период %K
        d_period: Период %D
        
    Returns:
        Словарь с массивами 'k', 'd'
    """
    high = ensure_numpy_array(high)
    low = ensure_numpy_array(low)
    close = ensure_numpy_array(close)
    
    if len(high) < k_period:
        logger.warning(f"Недостаточно данных для Stochastic")
        return {'k': np.array([]), 'd': np.array([])}
    
    k = np.zeros_like(high)
    k[:k_period-1] = np.nan
    
    for i in range(k_period-1, len(high)):
        high_max = np.max(high[i-k_period+1:i+1])
        low_min = np.min(low[i-k_period+1:i+1])
        
        if high_max - low_min != 0:
            k[i] = 100 * (close[i] - low_min) / (high_max - low_min)
        else:
            k[i] = 50
    
    # %D - SMA от %K
    d = np.zeros_like(high)
    d[:k_period + d_period - 2] = np.nan
    
    for i in range(k_period + d_period - 2, len(high)):
        if not np.isnan(k[i-d_period+1:i+1]).any():
            d[i] = np.mean(k[i-d_period+1:i+1])
        else:
            d[i] = np.nan
    
    return {'k': k, 'd': d}


# Для тестирования
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Создаем тестовые данные
    np.random.seed(42)
    test_data = 100 + np.cumsum(np.random.randn(100) * 2)
    
    print("Тестирование технических индикаторов...")
    
    # Тест SMA
    sma = calculate_sma(test_data, period=20)
    print(f"SMA: длина {len(sma)}, первые значения: {sma[:5]}")
    
    # Тест RSI
    rsi = calculate_rsi(test_data, period=14)
    print(f"RSI: длина {len(rsi)}, последние значения: {rsi[-5:]}")
    
    # Тест Bollinger Bands
    bb = calculate_bollinger_bands(test_data)
    print(f"Bollinger Bands: upper[{len(bb['upper'])}], lower[{len(bb['lower'])}]")
    
    # Тест MACD
    macd = calculate_macd(test_data)
    print(f"MACD: длина {len(macd['macd'])}")
    
    print("✅ Тестирование завершено")