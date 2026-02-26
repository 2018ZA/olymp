# screener/stock_screener.py
"""
Модуль для скрининга акций MOEX и поиска перспективных инструментов.
Адаптирован для работы с новым MoexClient на базе moexalgo.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from data.moex_client import MoexClient
from indicators.technical import (
    calculate_rsi, calculate_sma, calculate_ema,
    calculate_bollinger_bands, calculate_atr, calculate_macd
)
from config.trading_config import TRADING_CONFIG

logger = logging.getLogger(__name__)

class StockScreener:
    """
    Класс для скрининга акций MOEX.
    Анализирует технические индикаторы и выдает скоринговую оценку.
    """

    def __init__(self, max_workers: int = 5):
        """
        Инициализация скринера.
        
        Args:
            max_workers: Максимальное количество потоков для параллельной загрузки
        """
        self.client = MoexClient()
        self.max_workers = max_workers
        self.results = []
        self.tickers_list = TRADING_CONFIG.get('tickers', [])
        
        # Веса для скоринга
        self.weights = {
            'rsi': 0.25,
            'trend': 0.30,
            'volume': 0.15,
            'volatility': 0.10,
            'momentum': 0.20
        }

    def screen_all_tickers(self, days: int = 30, top_n: int = 10) -> pd.DataFrame:
        """
        Анализирует все тикеры из конфига и возвращает топ-N.
        
        Args:
            days: Количество дней истории для анализа
            top_n: Количество лучших акций для возврата
            
        Returns:
            DataFrame с результатами анализа
        """
        logger.info(f"Начало скрининга {len(self.tickers_list)} акций...")
        self.results = []
        
        # Параллельная загрузка и анализ
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_ticker = {
                executor.submit(self._analyze_ticker, ticker, days): ticker 
                for ticker in self.tickers_list
            }
            
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    result = future.result()
                    if result:
                        self.results.append(result)
                        logger.debug(f"Анализ {ticker} завершен")
                except Exception as e:
                    logger.error(f"Ошибка при анализе {ticker}: {e}")
        
        # Сортируем по оценке и возвращаем топ
        if self.results:
            df = pd.DataFrame(self.results)
            df = df.sort_values('score', ascending=False).reset_index(drop=True)
            logger.info(f"Скрининг завершен. Проанализировано {len(df)} акций")
            return df.head(top_n)
        else:
            logger.warning("Нет результатов анализа")
            return pd.DataFrame()

    def _analyze_ticker(self, ticker: str, days: int) -> Optional[Dict]:
        """
        Анализирует один тикер и возвращает результат.
        
        Args:
            ticker: Тикер для анализа
            days: Количество дней истории
            
        Returns:
            Словарь с результатами анализа или None
        """
        try:
            # Получаем исторические данные
            candles = self.client.get_candles(ticker, days=days)
            
            if candles.empty or len(candles) < 20:
                logger.debug(f"Недостаточно данных для {ticker}")
                return None
            
            # Получаем информацию о тикере
            info = self.client.get_ticker_info(ticker)
            
            # Рассчитываем индикаторы
            closes = candles['close'].values
            highs = candles['high'].values
            lows = candles['low'].values
            volumes = candles['volume'].values
            
            # Текущие значения
            current_price = closes[-1]
            current_volume = volumes[-1]
            
            # RSI
            rsi = calculate_rsi(closes, period=14)
            current_rsi = rsi[-1] if len(rsi) > 0 else 50
            
            # Скользящие средние
            sma_20 = calculate_sma(closes, period=20)
            sma_50 = calculate_sma(closes, period=50)
            current_sma_20 = sma_20[-1] if len(sma_20) > 0 else current_price
            current_sma_50 = sma_50[-1] if len(sma_50) > 0 else current_price
            
            # Определяем тренд
            if current_price > current_sma_20 and current_sma_20 > current_sma_50:
                trend = "up"
                trend_score = 1.0
            elif current_price < current_sma_20 and current_sma_20 < current_sma_50:
                trend = "down"
                trend_score = 0.0
            else:
                trend = "neutral"
                trend_score = 0.5
            
            # Полосы Боллинджера
            bb = calculate_bollinger_bands(closes, period=20, std_dev=2)
            bb_position = 0.5
            if bb and len(bb['upper']) > 0 and len(bb['lower']) > 0:
                upper = bb['upper'][-1]
                lower = bb['lower'][-1]
                if upper > lower:
                    bb_position = (current_price - lower) / (upper - lower)
            
            # ATR (волатильность)
            atr = calculate_atr(highs, lows, closes, period=14)
            current_atr = atr[-1] if len(atr) > 0 else 0
            atr_percent = (current_atr / current_price) * 100 if current_price > 0 else 0
            
            # Объем (сравниваем со средним)
            avg_volume = np.mean(volumes[-20:]) if len(volumes) >= 20 else current_volume
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1
            
            # MACD (импульс)
            macd = calculate_macd(closes)
            macd_signal = 0
            if macd and len(macd['macd']) > 0 and len(macd['signal']) > 0:
                if macd['macd'][-1] > macd['signal'][-1]:
                    macd_signal = 1  # Бычий
                elif macd['macd'][-1] < macd['signal'][-1]:
                    macd_signal = -1  # Медвежий
            
            # Расчет скоринга
            scores = self._calculate_scores(
                current_rsi=current_rsi,
                trend_score=trend_score,
                bb_position=bb_position,
                volume_ratio=volume_ratio,
                macd_signal=macd_signal,
                atr_percent=atr_percent
            )
            
            # Итоговая оценка
            total_score = sum(scores.values())
            
            # Определяем рекомендацию
            recommendation = self._get_recommendation(total_score, current_rsi, trend)
            
            return {
                'ticker': ticker,
                'name': info.get('name', ticker) if info else ticker,
                'sector': info.get('sector', 'unknown') if info else 'unknown',
                'price': round(current_price, 2),
                'rsi': round(current_rsi, 1),
                'trend': trend,
                'bb_position': round(bb_position, 2),
                'volume_ratio': round(volume_ratio, 2),
                'atr_percent': round(atr_percent, 2),
                'macd_signal': macd_signal,
                'score': round(total_score, 1),
                'recommendation': recommendation,
                'lot_size': info.get('lot_size', 1) if info else 1
            }
            
        except Exception as e:
            logger.error(f"Ошибка при анализе {ticker}: {e}")
            return None

    def _calculate_scores(self, current_rsi: float, trend_score: float,
                      bb_position: float, volume_ratio: float,
                      macd_signal: int, atr_percent: float) -> Dict[str, float]:
        """
        Рассчитывает оценки по каждому фактору.
        
        Returns:
            Словарь с оценками (каждая от 0 до 20-30, чтобы сумма была ~100)
        """
        # RSI: 0-30 - перепроданность (хорошо для покупки), 70-100 - перекупленность (плохо)
        if current_rsi < 30:
            rsi_score = 25 * (1 + (30 - current_rsi) / 30)  # 25-50 баллов
        elif current_rsi > 70:
            rsi_score = 5 * (1 - (current_rsi - 70) / 30)   # 0-5 баллов
        else:
            # В зоне 30-70: чем ближе к 30, тем лучше
            rsi_score = 20 * (1 - abs(current_rsi - 45) / 40)  # ~10-20 баллов
        
        rsi_score = max(0, min(50, rsi_score))
        
        # Тренд: up - 30 баллов, neutral - 15, down - 0
        trend_score_value = trend_score * 30  # trend_score уже 1.0 для up, 0.5 для neutral
        
        # Позиция в BB: у нижней границы (0) - 20 баллов, у верхней (1) - 0
        bb_score = 20 * (1 - bb_position)
        
        # Объем: выше среднего - бонус
        if volume_ratio > 1.5:
            volume_score = 15
        elif volume_ratio > 1.0:
            volume_score = 10
        elif volume_ratio > 0.7:
            volume_score = 5
        else:
            volume_score = 0
        
        # MACD: бычий сигнал - 15 баллов
        macd_score = 15 if macd_signal == 1 else 0
        
        # Волатильность: умеренная (2-4%) - хорошо
        if 2 <= atr_percent <= 4:
            volatility_score = 10
        elif atr_percent < 2:
            volatility_score = 5  # слишком тихо
        else:
            volatility_score = 0  # слишком волатильно
        
        scores = {
            'rsi': rsi_score,
            'trend': trend_score_value,
            'volume': volume_score,
            'volatility': volatility_score,
            'momentum': macd_score
        }
        
        return scores

    def _get_recommendation(self, score: float, rsi: float, trend: str) -> str:
        """
        Определяет рекомендацию на основе оценки.
        
        Args:
            score: Итоговая оценка
            rsi: Значение RSI
            trend: Направление тренда
            
        Returns:
            Строка с рекомендацией
        """
        if score >= 4.0:
            return "🚀 СИЛЬНАЯ ПОКУПКА"
        elif score >= 3.0:
            if rsi < 40 and trend == "up":
                return "✅ ПОКУПКА"
            else:
                return "👀 НАБЛЮДЕНИЕ"
        elif score >= 2.0:
            return "📊 НЕЙТРАЛЬНО"
        else:
            return "❌ ИЗБЕГАТЬ"

    def screen_by_strategy(self, strategy_name: str, days: int = 30) -> pd.DataFrame:
        """
        Анализирует акции для конкретной стратегии.
        
        Args:
            strategy_name: Название стратегии ('rsi', 'sma', 'momentum', 'value')
            days: Количество дней истории
            
        Returns:
            DataFrame с акциями, подходящими под стратегию
        """
        # Получаем все результаты
        all_results = self.screen_all_tickers(days=days, top_n=len(self.tickers_list))
        
        if all_results.empty:
            return all_results
        
        # Фильтруем по стратегии
        if strategy_name == 'rsi':
            # RSI Mean Reversion: ищем перепроданные
            mask = (all_results['rsi'] < 40) | (all_results['rsi'] > 70)
            filtered = all_results[mask].copy()
            filtered['strategy_note'] = filtered['rsi'].apply(
                lambda x: '📉 Перепроданность' if x < 40 else '📈 Перекупленность'
            )
            
        elif strategy_name == 'sma':
            # SMA Crossover: ищем акции выше 20 SMA и 20 SMA выше 50 SMA
            # Для этого нужны дополнительные данные, упростим
            filtered = all_results[all_results['trend'] == 'up'].copy()
            filtered['strategy_note'] = '📈 Восходящий тренд'
            
        elif strategy_name == 'momentum':
            # Momentum: ищем сильный импульс
            filtered = all_results[all_results['macd_signal'] == 1].copy()
            filtered['strategy_note'] = '⚡ Бычий MACD'
            
        elif strategy_name == 'value':
            # Value: ищем недооцененные по RSI и позиции в BB
            filtered = all_results[
                (all_results['rsi'] < 45) & 
                (all_results['bb_position'] < 0.3)
            ].copy()
            filtered['strategy_note'] = '💰 Потенциально недооценена'
            
        else:
            filtered = all_results.copy()
            filtered['strategy_note'] = '📊 Общий анализ'
        
        # Сортируем по оценке
        return filtered.sort_values('score', ascending=False).reset_index(drop=True)

    def find_trading_pairs(self, sector: str = None, days: int = 30) -> List[Dict]:
        """
        Находит потенциальные пары для парного трейдинга.
        
        Args:
            sector: Сектор для поиска пар (если None, ищем по всем)
            days: Количество дней истории
            
        Returns:
            Список словарей с информацией о парах
        """
        # Получаем все результаты
        results_df = self.screen_all_tickers(days=days, top_n=len(self.tickers_list))
        
        if results_df.empty:
            return []
        
        # Фильтруем по сектору, если нужно
        if sector:
            results_df = results_df[results_df['sector'] == sector]
        
        if len(results_df) < 2:
            return []
        
        # Группируем по секторам
        pairs = []
        sectors = results_df['sector'].unique()
        
        for sector in sectors:
            sector_stocks = results_df[results_df['sector'] == sector]
            if len(sector_stocks) >= 2:
                # Берем топ-2 по оценке в секторе
                top_stocks = sector_stocks.nlargest(2, 'score')
                if len(top_stocks) >= 2:
                    ticker1, ticker2 = top_stocks.iloc[0]['ticker'], top_stocks.iloc[1]['ticker']
                    
                    # Получаем цены для расчета спреда
                    data1 = self.client.get_candles(ticker1, days=days)
                    data2 = self.client.get_candles(ticker2, days=days)
                    
                    if not data1.empty and not data2.empty:
                        # Объединяем по времени
                        merged = pd.merge(
                            data1[['begin', 'close']].rename(columns={'close': 'close1'}),
                            data2[['begin', 'close']].rename(columns={'close': 'close2'}),
                            on='begin', how='inner'
                        )
                        
                        if len(merged) > 5:
                            # Рассчитываем спред
                            merged['spread'] = merged['close1'] - merged['close2']
                            spread_std = merged['spread'].std()
                            spread_mean = merged['spread'].mean()
                            current_spread = merged['spread'].iloc[-1]
                            
                            z_score = (current_spread - spread_mean) / spread_std if spread_std > 0 else 0
                            
                            # Оцениваем силу пары
                            if abs(z_score) > 2:
                                strength = "💪 СИЛЬНАЯ"
                            elif abs(z_score) > 1:
                                strength = "👌 СРЕДНЯЯ"
                            else:
                                strength = "🤏 СЛАБАЯ"
                            
                            pairs.append({
                                'sector': sector,
                                'asset1': ticker1,
                                'asset2': ticker2,
                                'spread': round(spread_mean, 2),
                                'current_z': round(z_score, 2),
                                'strength': strength,
                                'score1': top_stocks.iloc[0]['score'],
                                'score2': top_stocks.iloc[1]['score']
                            })
        
        return sorted(pairs, key=lambda x: abs(x['current_z']), reverse=True)


# Для тестирования
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    screener = StockScreener(max_workers=3)
    
    # Тест 1: Топ-5 акций
    print("\n" + "="*70)
    print("ТЕСТ 1: ТОП-5 АКЦИЙ")
    print("="*70)
    top5 = screener.screen_all_tickers(days=30, top_n=5)
    if not top5.empty:
        print(top5[['ticker', 'name', 'price', 'rsi', 'trend', 'score', 'recommendation']].to_string(index=False))
    
    # Тест 2: Акции для RSI стратегии
    print("\n" + "="*70)
    print("ТЕСТ 2: АКЦИИ ДЛЯ RSI СТРАТЕГИИ")
    print("="*70)
    rsi_stocks = screener.screen_by_strategy('rsi', days=30)
    if not rsi_stocks.empty:
        print(rsi_stocks[['ticker', 'rsi', 'strategy_note', 'score']].head(5).to_string(index=False))
    
    # Тест 3: Парный трейдинг
    print("\n" + "="*70)
    print("ТЕСТ 3: ПОТЕНЦИАЛЬНЫЕ ПАРЫ")
    print("="*70)
    pairs = screener.find_trading_pairs(days=30)
    if pairs:
        for pair in pairs[:3]:  # Первые 3 пары
            print(f"  {pair['sector']}: {pair['asset1']} / {pair['asset2']} | "
                  f"Z-score: {pair['current_z']} | {pair['strength']}")
    else:
        print("  Пар не найдено")
    
    print("\n" + "="*70)
    print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("="*70)