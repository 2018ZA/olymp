"""
Технические индикаторы для торговых стратегий.
"""

import numpy as np
import pandas as pd
from typing import Union, List, Tuple


def calculate_sma(prices: Union[List[float], np.ndarray, pd.Series], period: int) -> np.ndarray:
    """
    Рассчитывает простую скользящую среднюю (SMA).
    
    Args:
        prices: Цены закрытия
        period: Период SMA
        
    Returns:
        Массив значений SMA (с NaN в начале)
    """
    prices = np.array(prices)
    if len(prices) < period:
        # Возвращаем массив из NaN той же длины, что и prices
        return np.full(len(prices), np.nan)
    
    sma = np.convolve(prices, np.ones(period), 'valid') / period
    # Добавляем NaN в начало для выравнивания длины
    return np.concatenate([np.full(period - 1, np.nan), sma])


def calculate_ema(prices: Union[List[float], np.ndarray, pd.Series], period: int) -> np.ndarray:
    """
    Рассчитывает экспоненциальную скользящую среднюю (EMA).
    
    Args:
        prices: Цены закрытия
        period: Период EMA
        
    Returns:
        Массив значений EMA (с NaN в начале)
    """
    prices = np.array(prices)
    if len(prices) < period:
        return np.full(len(prices), np.nan)
    
    ema = np.zeros_like(prices, dtype=float)
    ema[:period] = np.nan
    
    # Первое значение EMA - простое среднее первых period значений
    ema[period - 1] = np.mean(prices[:period])
    
    # Коэффициент сглаживания
    multiplier = 2 / (period + 1)
    
    # Рассчитываем EMA для остальных значений
    for i in range(period, len(prices)):
        if np.isnan(ema[i-1]):
            ema[i] = prices[i]
        else:
            ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]
    
    return ema


def calculate_rsi(prices: Union[List[float], np.ndarray, pd.Series], period: int = 14) -> float:
    """
    Рассчитывает индекс относительной силы (RSI).
    
    Args:
        prices: Цены закрытия
        period: Период RSI
        
    Returns:
        Текущее значение RSI или 50.0 при недостатке данных
    """
    prices = np.array(prices)
    if len(prices) < period + 1:
        return 50.0  # Возвращаем нейтральное значение
    
    # Рассчитываем изменения цен
    deltas = np.diff(prices)
    
    # Берем последние period+1 изменений
    if len(deltas) > period:
        deltas = deltas[-period:]
    
    # Разделяем на положительные и отрицательные изменения
    gains = deltas[deltas > 0]
    losses = -deltas[deltas < 0]
    
    # Средние значения
    avg_gain = np.mean(gains) if len(gains) > 0 else 0.0001
    avg_loss = np.mean(losses) if len(losses) > 0 else 0.0001
    
    # Рассчитываем RS и RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_atr(high: Union[List[float], np.ndarray, pd.Series], 
                  low: Union[List[float], np.ndarray, pd.Series], 
                  close: Union[List[float], np.ndarray, pd.Series], 
                  period: int = 14) -> float:
    """
    Рассчитывает средний истинный диапазон (ATR).
    
    Args:
        high: Максимальные цены
        low: Минимальные цены
        close: Цены закрытия
        period: Период ATR
        
    Returns:
        Текущее значение ATR или 0.0 при недостатке данных
    """
    high = np.array(high)
    low = np.array(low)
    close = np.array(close)
    
    if len(high) < period + 1:
        return 0.0
    
    # Рассчитываем истинный диапазон
    tr1 = high[1:] - low[1:]
    tr2 = np.abs(high[1:] - close[:-1])
    tr3 = np.abs(low[1:] - close[:-1])
    
    tr = np.maximum(np.maximum(tr1, tr2), tr3)
    
    # Берем последние period значений
    if len(tr) >= period:
        tr = tr[-period:]
    else:
        return 0.0
    
    return float(np.mean(tr))


def calculate_bollinger_bands(prices: Union[List[float], np.ndarray, pd.Series], 
                              period: int = 20, 
                              num_std: float = 2.0) -> Tuple[float, float, float]:
    """
    Рассчитывает полосы Боллинджера.
    
    Args:
        prices: Цены закрытия
        period: Период для SMA
        num_std: Количество стандартных отклонений
        
    Returns:
        Кортеж (верхняя полоса, средняя полоса, нижняя полоса)
    """
    prices = np.array(prices)
    if len(prices) < period:
        return np.nan, np.nan, np.nan
    
    sma_values = calculate_sma(prices, period)
    if np.isnan(sma_values[-1]):
        return np.nan, np.nan, np.nan
    
    sma = sma_values[-1]
    std = np.std(prices[-period:])
    
    upper_band = sma + (std * num_std)
    lower_band = sma - (std * num_std)
    
    return upper_band, sma, lower_band


def calculate_macd(prices: Union[List[float], np.ndarray, pd.Series],
                   fast_period: int = 12,
                   slow_period: int = 26,
                   signal_period: int = 9) -> Tuple[float, float, float]:
    """
    Рассчитывает MACD.
    
    Args:
        prices: Цены закрытия
        fast_period: Период быстрой EMA
        slow_period: Период медленной EMA
        signal_period: Период сигнальной линии
        
    Returns:
        Кортеж (MACD линия, сигнальная линия, гистограмма)
    """
    prices = np.array(prices)
    
    if len(prices) < max(fast_period, slow_period) + signal_period:
        return 0.0, 0.0, 0.0
    
    ema_fast = calculate_ema(prices, fast_period)
    ema_slow = calculate_ema(prices, slow_period)
    
    # Проверяем на NaN
    if np.isnan(ema_fast[-1]) or np.isnan(ema_slow[-1]):
        return 0.0, 0.0, 0.0
    
    macd_line = ema_fast - ema_slow
    
    # Убираем NaN для расчета сигнальной линии
    valid_macd = macd_line[~np.isnan(macd_line)]
    if len(valid_macd) < signal_period:
        return 0.0, 0.0, 0.0
    
    signal_line_values = calculate_ema(valid_macd, signal_period)
    if len(signal_line_values) == 0 or np.isnan(signal_line_values[-1]):
        return 0.0, 0.0, 0.0
    
    signal_line = signal_line_values[-1]
    macd_value = macd_line[-1] if not np.isnan(macd_line[-1]) else 0.0
    
    histogram = macd_value - signal_line
    
    return macd_value, signal_line, histogram