"""
Главный класс для скрининга акций.
Анализирует все доступные инструменты и оценивает их перспективность.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

from data.moex_client import MoexClient
from indicators.technical import (
    calculate_sma, calculate_ema, calculate_rsi, 
    calculate_atr, calculate_bollinger_bands, calculate_macd
)
from utils.logger import logger
from utils.validators import validate_market_data


@dataclass
class StockScore:
    """Класс для хранения оценки акции"""
    ticker: str
    name: str = ""  # Название компании
    sector: str = ""  # Сектор экономики
    
    # Метрики
    price: float = 0.0
    volume: float = 0.0
    market_cap: float = 0.0
    
    # Индикаторы
    rsi: float = 50.0
    macd: float = 0.0
    macd_signal: float = 0.0
    macd_histogram: float = 0.0
    sma_20: float = 0.0
    sma_50: float = 0.0
    bb_upper: float = 0.0
    bb_lower: float = 0.0
    bb_position: float = 0.5  # Позиция в полосах Боллинджера (0-1)
    atr: float = 0.0
    atr_percent: float = 0.0  # ATR в процентах от цены
    
    # Тренды
    trend_short: str = "neutral"  # up/down/neutral
    trend_medium: str = "neutral"
    trend_long: str = "neutral"
    
    # Сила сигналов
    momentum_score: float = 0.0  # -1 до 1
    trend_score: float = 0.0     # -1 до 1
    volume_score: float = 0.0    # 0 до 1
    volatility_score: float = 0.0 # 0 до 1 (чем ниже, тем лучше)
    
    # Итоговая оценка
    total_score: float = 0.0      # 0 до 100
    recommendation: str = "HOLD"  # BUY, STRONG_BUY, HOLD, SELL, STRONG_SELL
    
    # Подходящие стратегии
    suitable_strategies: List[str] = field(default_factory=list)
    
    # Время анализа
    timestamp: datetime = field(default_factory=datetime.now)


class StockScreener:
    """
    Скринер для оценки перспективности акций.
    Анализирует технические индикаторы и выдает рекомендации.
    """
    
    def __init__(self):
        self.data_client = MoexClient()
        self.tickers = self._get_all_tickers()
        self.scores: Dict[str, StockScore] = {}
        
        # Веса для итоговой оценки
        self.weights = {
            'momentum': 0.35,
            'trend': 0.30,
            'volume': 0.20,
            'volatility': 0.15
        }
        
        logger.info(f"StockScreener инициализирован. Доступно тикеров: {len(self.tickers)}")
    
    def _get_all_tickers(self) -> List[str]:
        """
        Получает список всех доступных тикеров.
        В реальном проекте можно загружать из API MOEX.
        """
        # Базовый список ликвидных акций
        return [
            'GAZP', 'SBER', 'SBERP', 'LKOH', 'ROSN', 'TATN', 'NVTK',
            'GMKN', 'PLZL', 'NLMK', 'CHMF', 'ALRS', 'MTLR',
            'YDEX', 'VKCO', 'MTSS', 'POSI', 'HEAD',
            'AFLT', 'FLOT',
            'MOEX', 'SVCB', 'T', 'PIKK', 'MGNT', 'X5'
        ]
    
    def _get_company_name(self, ticker: str) -> str:
        """Возвращает название компании по тикеру"""
        names = {
            'GAZP': 'Газпром',
            'SBER': 'Сбербанк',
            'SBERP': 'Сбербанк (преф)',
            'LKOH': 'Лукойл',
            'ROSN': 'Роснефть',
            'TATN': 'Татнефть',
            'NVTK': 'НОВАТЭК',
            'GMKN': 'Норникель',
            'PLZL': 'Полюс',
            'NLMK': 'НЛМК',
            'CHMF': 'Северсталь',
            'ALRS': 'АЛРОСА',
            'MTLR': 'Мечел',
            'YDEX': 'Яндекс',
            'VKCO': 'VK',
            'MTSS': 'МТС',
            'POSI': 'Positive Technologies',
            'HEAD': 'HeadHunter',
            'AFLT': 'Аэрофлот',
            'FLOT': 'Совкомфлот',
            'MOEX': 'Мосбиржа',
            'SVCB': 'Совкомбанк',
            'T': 'Т-Технологии',
            'PIKK': 'ПИК',
            'MGNT': 'Магнит',
            'X5': 'X5 Retail'
        }
        return names.get(ticker, ticker)
    
    def _get_sector(self, ticker: str) -> str:
        """Определяет сектор экономики по тикеру"""
        sectors = {
            # Нефтегаз
            'GAZP': 'Нефтегаз', 'LKOH': 'Нефтегаз', 'ROSN': 'Нефтегаз',
            'TATN': 'Нефтегаз', 'NVTK': 'Нефтегаз',
            
            # Финансы
            'SBER': 'Финансы', 'SBERP': 'Финансы', 'MOEX': 'Финансы',
            'SVCB': 'Финансы', 'T': 'Финансы',
            
            # Металлы
            'GMKN': 'Металлы', 'PLZL': 'Металлы', 'NLMK': 'Металлы',
            'CHMF': 'Металлы', 'ALRS': 'Металлы', 'MTLR': 'Металлы',
            
            # IT
            'YDEX': 'IT', 'VKCO': 'IT', 'MTSS': 'IT', 'POSI': 'IT', 'HEAD': 'IT',
            
            # Транспорт
            'AFLT': 'Транспорт', 'FLOT': 'Транспорт',
            
            # Потребсектор
            'PIKK': 'Строительство', 'MGNT': 'Ритейл', 'X5': 'Ритейл'
        }
        return sectors.get(ticker, 'Другое')
    
    def analyze_ticker(self, ticker: str, days: int = 30) -> Optional[StockScore]:
        """
        Анализирует один тикер и возвращает оценку.
        
        Args:
            ticker: Тикер акции
            days: Количество дней для анализа
            
        Returns:
            StockScore или None при ошибке
        """
        try:
            # Получаем данные
            df = self.data_client.get_history(ticker, days)
            if df.empty or not validate_market_data(df):
                logger.warning(f"Нет данных для {ticker}")
                return None
            
            # Берем последние 50 свечей для расчетов
            data = df.tail(50).copy()
            closes = data['close'].values
            highs = data['high'].values
            lows = data['low'].values
            volumes = data['volume'].values
            
            current_price = closes[-1]
            prev_price = closes[-2] if len(closes) > 1 else current_price
            
            # Создаем объект оценки
            score = StockScore(
                ticker=ticker,
                name=self._get_company_name(ticker),
                sector=self._get_sector(ticker),
                price=current_price,
                volume=volumes[-1],
                market_cap=current_price * 1_000_000_000  # Упрощенно
            )
            
            # 1. RSI
            score.rsi = calculate_rsi(closes, 14)
            # Проверка на NaN
            if np.isnan(score.rsi):
                score.rsi = 50.0
            
            # 2. MACD
            macd_line, signal_line, histogram = calculate_macd(closes)
            score.macd = macd_line if not np.isnan(macd_line) else 0.0
            score.macd_signal = signal_line if not np.isnan(signal_line) else 0.0
            score.macd_histogram = histogram if not np.isnan(histogram) else 0.0
            
            # 3. Скользящие средние
            sma_20 = calculate_sma(closes, 20)
            sma_50 = calculate_sma(closes, 50)
            
            if len(sma_20) > 0 and not np.isnan(sma_20[-1]):
                score.sma_20 = sma_20[-1]
            else:
                score.sma_20 = current_price
                
            if len(sma_50) > 0 and not np.isnan(sma_50[-1]):
                score.sma_50 = sma_50[-1]
            else:
                score.sma_50 = current_price
            
            # 4. Полосы Боллинджера
            bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(closes)
            
            if not np.isnan(bb_upper):
                score.bb_upper = bb_upper
            if not np.isnan(bb_lower):
                score.bb_lower = bb_lower
                
            if (not np.isnan(bb_upper) and not np.isnan(bb_lower) and 
                bb_upper > bb_lower and not np.isnan(current_price)):
                score.bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
                # Ограничиваем позицию от 0 до 1
                score.bb_position = max(0.0, min(1.0, score.bb_position))
            else:
                score.bb_position = 0.5
            
            # 5. ATR
            score.atr = calculate_atr(highs, lows, closes, 14)
            if np.isnan(score.atr) or score.atr < 0:
                score.atr = 0.0
                
            score.atr_percent = (score.atr / current_price * 100) if current_price > 0 else 0.0
            if np.isnan(score.atr_percent):
                score.atr_percent = 0.0
            
            # 6. Определяем тренды
            # Краткосрочный тренд (5 дней)
            if len(closes) >= 5:
                sma_5 = np.mean(closes[-5:])
                if not np.isnan(sma_5):
                    score.trend_short = 'up' if current_price > sma_5 else 'down'
            
            # Среднесрочный тренд (относительно SMA 20)
            if score.sma_20 > 0:
                if current_price > score.sma_20 * 1.02:
                    score.trend_medium = 'up'
                elif current_price < score.sma_20 * 0.98:
                    score.trend_medium = 'down'
            
            # Долгосрочный тренд (относительно SMA 50)
            if score.sma_50 > 0:
                if current_price > score.sma_50 * 1.05:
                    score.trend_long = 'up'
                elif current_price < score.sma_50 * 0.95:
                    score.trend_long = 'down'
            
            # 7. Рассчитываем скоринг
            
            # Momentum score (RSI + MACD + позиция в BB)
            # RSI: 30-70 норма, ниже 30 перепродано (хорошо для покупки), выше 70 перекуплено
            if score.rsi < 30:
                momentum_rsi = 1.0  # Сильный сигнал на покупку
            elif score.rsi < 40:
                momentum_rsi = 0.7
            elif score.rsi < 50:
                momentum_rsi = 0.3
            elif score.rsi < 60:
                momentum_rsi = 0.0
            elif score.rsi < 70:
                momentum_rsi = -0.3
            else:
                momentum_rsi = -1.0  # Сильный сигнал на продажу
            
            # MACD: гистограмма > 0 - бычий сигнал
            momentum_macd = 1.0 if score.macd_histogram > 0 else -1.0
            
            # Позиция в BB: ниже нижней полосы - перепродано
            if score.bb_position < 0.1:
                momentum_bb = 1.0
            elif score.bb_position < 0.3:
                momentum_bb = 0.5
            elif score.bb_position < 0.7:
                momentum_bb = 0.0
            elif score.bb_position < 0.9:
                momentum_bb = -0.5
            else:
                momentum_bb = -1.0
            
            score.momentum_score = (momentum_rsi * 0.4 + momentum_macd * 0.3 + momentum_bb * 0.3)
            
            # Trend score
            trend_map = {'up': 1, 'neutral': 0, 'down': -1}
            trend_short_score = trend_map.get(score.trend_short, 0)
            trend_medium_score = trend_map.get(score.trend_medium, 0)
            trend_long_score = trend_map.get(score.trend_long, 0)
            
            score.trend_score = (trend_short_score * 0.3 + trend_medium_score * 0.3 + trend_long_score * 0.4)
            
            # Volume score (текущий объем к среднему)
            if len(volumes) >= 10:
                avg_volume = np.mean(volumes[-10:])
                if avg_volume > 0:
                    volume_ratio = volumes[-1] / avg_volume
                    if volume_ratio > 1.5:
                        score.volume_score = 1.0
                    elif volume_ratio > 1.2:
                        score.volume_score = 0.7
                    elif volume_ratio > 0.8:
                        score.volume_score = 0.3
                    else:
                        score.volume_score = 0.0
                else:
                    score.volume_score = 0.0
            else:
                score.volume_score = 0.0
            
            # Volatility score (чем ниже ATR%, тем лучше для входа)
            if score.atr_percent < 1.0:
                score.volatility_score = 1.0
            elif score.atr_percent < 2.0:
                score.volatility_score = 0.7
            elif score.atr_percent < 3.0:
                score.volatility_score = 0.3
            else:
                score.volatility_score = 0.0
            
            # Итоговая оценка (0-100)
            raw_score = (
                (score.momentum_score + 1) / 2 * 100 * self.weights['momentum'] +
                (score.trend_score + 1) / 2 * 100 * self.weights['trend'] +
                score.volume_score * 100 * self.weights['volume'] +
                score.volatility_score * 100 * self.weights['volatility']
            )
            
            score.total_score = min(100, max(0, raw_score))
            
            # Рекомендация
            if score.total_score >= 80:
                score.recommendation = "STRONG_BUY"
            elif score.total_score >= 60:
                score.recommendation = "BUY"
            elif score.total_score >= 40:
                score.recommendation = "HOLD"
            elif score.total_score >= 20:
                score.recommendation = "SELL"
            else:
                score.recommendation = "STRONG_SELL"
            
            return score
            
        except Exception as e:
            logger.error(f"Ошибка анализа {ticker}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def analyze_all(self, limit: int = None, min_score: float = 0) -> List[StockScore]:
        """
        Анализирует все доступные акции.
        
        Args:
            limit: Максимальное количество акций для анализа
            min_score: Минимальная оценка для включения в результат
            
        Returns:
            Список оценок акций
        """
        tickers_to_analyze = self.tickers[:limit] if limit else self.tickers
        results = []
        
        logger.info(f"Начинаем анализ {len(tickers_to_analyze)} акций...")
        
        for ticker in tickers_to_analyze:
            score = self.analyze_ticker(ticker)
            if score and score.total_score >= min_score:
                results.append(score)
            
            # Небольшая пауза, чтобы не нагружать API
            import time
            time.sleep(0.5)
        
        # Сортируем по убыванию оценки
        results.sort(key=lambda x: x.total_score, reverse=True)
        
        self.scores = {s.ticker: s for s in results}
        logger.info(f"Анализ завершен. Найдено {len(results)} акций с оценкой >{min_score}")
        
        return results
    
    def get_top_stocks(self, n: int = 10) -> List[StockScore]:
        """
        Возвращает топ N акций для покупки.
        
        Args:
            n: Количество акций в топе
            
        Returns:
            Список лучших акций
        """
        if not self.scores:
            self.analyze_all()
        
        # Берем акции с рекомендацией BUY или STRONG_BUY
        top = [s for s in self.scores.values() if s.recommendation in ['BUY', 'STRONG_BUY']]
        top.sort(key=lambda x: x.total_score, reverse=True)
        
        return top[:n]
    
    def get_stocks_by_sector(self, sector: str) -> List[StockScore]:
        """
        Возвращает акции из указанного сектора.
        
        Args:
            sector: Название сектора
            
        Returns:
            Список акций сектора
        """
        if not self.scores:
            self.analyze_all()
        
        return [s for s in self.scores.values() if s.sector == sector]