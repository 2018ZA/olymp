# screener/strategy_matcher.py
"""
Модуль для подбора акций под конкретные торговые стратегии.
Работает в связке с обновленным StockScreener на базе moexalgo.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class StrategyMatcher:
    """
    Класс для подбора акций, подходящих под различные торговые стратегии.
    Использует результаты анализа StockScreener для фильтрации и ранжирования.
    """

    def __init__(self, screener_results: pd.DataFrame = None):
        """
        Инициализация matcher'а.
        
        Args:
            screener_results: DataFrame с результатами анализа от StockScreener
        """
        self.results = screener_results if screener_results is not None else pd.DataFrame()
        self.strategy_results = {}

    def set_results(self, screener_results: pd.DataFrame):
        """
        Устанавливает результаты анализа для работы.
        
        Args:
            screener_results: DataFrame с результатами от StockScreener
        """
        self.results = screener_results
        logger.info(f"Загружены результаты для {len(self.results)} акций")

    def match_rsi_strategy(self, oversold_threshold: float = 40, 
                          overbought_threshold: float = 70) -> pd.DataFrame:
        """
        Находит акции для RSI Mean Reversion стратегии.
        
        Args:
            oversold_threshold: Порог перепроданности
            overbought_threshold: Порог перекупленности
            
        Returns:
            DataFrame с акциями, отсортированными по привлекательности для RSI стратегии
        """
        if self.results.empty:
            logger.warning("Нет данных для анализа RSI стратегии")
            return pd.DataFrame()

        # Копируем данные, чтобы не изменять оригинал
        df = self.results.copy()
        
        # Рассчитываем RSI score
        df['rsi_score'] = df['rsi'].apply(
            lambda x: self._calculate_rsi_strategy_score(x, oversold_threshold, overbought_threshold)
        )
        
        # Добавляем пояснения
        df['rsi_signal'] = df['rsi'].apply(
            lambda x: self._get_rsi_signal(x, oversold_threshold, overbought_threshold)
        )
        
        # Сортируем по RSI score
        result = df.sort_values('rsi_score', ascending=False)
        
        self.strategy_results['rsi'] = result
        logger.info(f"RSI стратегия: найдено {len(result)} акций")
        
        return result

    def _calculate_rsi_strategy_score(self, rsi: float, 
                                      oversold: float, 
                                      overbought: float) -> float:
        """
        Рассчитывает оценку для RSI стратегии.
        Чем ближе к oversold (для покупки) или к overbought (для продажи), тем выше оценка.
        
        Args:
            rsi: Значение RSI
            oversold: Порог перепроданности
            overbought: Порог перекупленности
            
        Returns:
            Оценка от 0 до 100
        """
        if rsi <= oversold:
            # Перепроданность: чем ниже RSI, тем лучше для покупки
            return 100 * (1 - rsi / oversold)
        elif rsi >= overbought:
            # Перекупленность: чем выше RSI, тем лучше для продажи
            return 100 * ((rsi - overbought) / (100 - overbought))
        else:
            # В нейтральной зоне
            if rsi < 50:
                # Ближе к перепроданности
                return 30 * (1 - (rsi - oversold) / (50 - oversold))
            else:
                # Ближе к перекупленности
                return 30 * ((rsi - 50) / (overbought - 50))

    def _get_rsi_signal(self, rsi: float, oversold: float, overbought: float) -> str:
        """Возвращает текстовый сигнал на основе RSI."""
        if rsi <= oversold:
            return "📈 СИГНАЛ К ПОКУПКЕ (перепроданность)"
        elif rsi >= overbought:
            return "📉 СИГНАЛ К ПРОДАЖЕ (перекупленность)"
        elif rsi < 45:
            return "👀 Близко к перепроданности"
        elif rsi > 55:
            return "👀 Близко к перекупленности"
        else:
            return "➡️ Нейтральная зона"

    def match_sma_strategy(self) -> pd.DataFrame:
        """
        Находит акции для SMA Crossover стратегии.
        Использует данные о тренде из скринера.
        
        Returns:
            DataFrame с акциями для SMA стратегии
        """
        if self.results.empty:
            logger.warning("Нет данных для анализа SMA стратегии")
            return pd.DataFrame()

        df = self.results.copy()
        
        # Для SMA стратегии важны: тренд и импульс
        df['sma_score'] = 0.0
        
        # Оценка на основе тренда
        df.loc[df['trend'] == 'up', 'sma_score'] += 50
        df.loc[df['trend'] == 'neutral', 'sma_score'] += 25
        
        # Оценка на основе MACD
        df.loc[df['macd_signal'] == 1, 'sma_score'] += 30
        df.loc[df['macd_signal'] == -1, 'sma_score'] -= 20
        
        # Оценка на основе позиции в BB
        df['bb_trend_score'] = df['bb_position'].apply(
            lambda x: 20 * x if x > 0.5 else 0
        )
        df['sma_score'] += df['bb_trend_score']
        
        df['sma_signal'] = df.apply(self._get_sma_signal, axis=1)
        
        result = df.sort_values('sma_score', ascending=False)
        
        self.strategy_results['sma'] = result
        logger.info(f"SMA стратегия: найдено {len(result)} акций")
        
        return result

    def _get_sma_signal(self, row) -> str:
        """Возвращает сигнал для SMA стратегии."""
        if row['trend'] == 'up' and row['macd_signal'] == 1:
            return "🚀 СИЛЬНЫЙ ВОСХОДЯЩИЙ ТРЕНД"
        elif row['trend'] == 'up':
            return "📈 Восходящий тренд"
        elif row['trend'] == 'down' and row['macd_signal'] == -1:
            return "📉 Нисходящий тренд"
        elif row['trend'] == 'down':
            return "⬇️ Слабый нисходящий тренд"
        else:
            return "➡️ Боковой тренд"

    def match_momentum_strategy(self) -> pd.DataFrame:
        """
        Находит акции для Momentum стратегии.
        
        Returns:
            DataFrame с акциями для Momentum стратегии
        """
        if self.results.empty:
            logger.warning("Нет данных для анализа Momentum стратегии")
            return pd.DataFrame()

        df = self.results.copy()
        
        # Для Momentum важны: MACD, объем, RSI в правильном диапазоне
        df['momentum_score'] = 0.0
        
        # MACD сигнал
        df.loc[df['macd_signal'] == 1, 'momentum_score'] += 40
        df.loc[df['macd_signal'] == -1, 'momentum_score'] -= 20
        
        # Объем (выше среднего - хорошо для momentum)
        df['volume_score'] = df['volume_ratio'].apply(
            lambda x: min(30, x * 15) if x > 1 else x * 10
        )
        df['momentum_score'] += df['volume_score']
        
        # RSI в зоне импульса (40-70)
        df['rsi_momentum_score'] = df['rsi'].apply(
            lambda x: 20 * (x - 40) / 30 if 40 <= x <= 70 else 0
        )
        df['momentum_score'] += df['rsi_momentum_score']
        
        df['momentum_signal'] = df.apply(self._get_momentum_signal, axis=1)
        
        result = df.sort_values('momentum_score', ascending=False)
        
        self.strategy_results['momentum'] = result
        logger.info(f"Momentum стратегия: найдено {len(result)} акций")
        
        return result

    def _get_momentum_signal(self, row) -> str:
        """Возвращает сигнал для Momentum стратегии."""
        if row['macd_signal'] == 1 and row['volume_ratio'] > 1.5:
            return "⚡ СИЛЬНЫЙ ИМПУЛЬС"
        elif row['macd_signal'] == 1:
            return "📊 Импульс вверх"
        elif row['macd_signal'] == -1:
            return "📉 Импульс вниз"
        else:
            return "➡️ Без импульса"

    def match_value_strategy(self) -> pd.DataFrame:
        """
        Находит акции для Value стратегии (недооцененные).
        
        Returns:
            DataFrame с акциями для Value стратегии
        """
        if self.results.empty:
            logger.warning("Нет данных для анализа Value стратегии")
            return pd.DataFrame()

        df = self.results.copy()
        
        # Для Value важны: низкий RSI, позиция у нижней границы BB
        df['value_score'] = 0.0
        
        # RSI (ниже 45 - потенциально недооценено)
        df['rsi_value_score'] = df['rsi'].apply(
            lambda x: 40 * (1 - x/45) if x < 45 else 0
        )
        df['value_score'] += df['rsi_value_score']
        
        # Позиция в BB (ближе к нижней границе - лучше)
        df['bb_value_score'] = df['bb_position'].apply(
            lambda x: 40 * (1 - x) if x < 0.5 else 20 * (1 - x)
        )
        df['value_score'] += df['bb_value_score']
        
        # Низкая волатильность - плюс для value
        df.loc[df['atr_percent'] < 2, 'value_score'] += 20
        
        df['value_signal'] = df.apply(self._get_value_signal, axis=1)
        
        result = df.sort_values('value_score', ascending=False)
        
        self.strategy_results['value'] = result
        logger.info(f"Value стратегия: найдено {len(result)} акций")
        
        return result

    def _get_value_signal(self, row) -> str:
        """Возвращает сигнал для Value стратегии."""
        if row['rsi'] < 35 and row['bb_position'] < 0.2:
            return "💰 СИЛЬНО НЕДООЦЕНЕНА"
        elif row['rsi'] < 40 and row['bb_position'] < 0.3:
            return "💎 ПОТЕНЦИАЛЬНО НЕДООЦЕНЕНА"
        elif row['rsi'] > 60:
            return "⚠️ Переоценена"
        else:
            return "📊 Справедливая оценка"

    def get_best_for_strategy(self, strategy: str, top_n: int = 5) -> pd.DataFrame:
        """
        Возвращает лучшие акции для указанной стратегии.
        
        Args:
            strategy: Название стратегии ('rsi', 'sma', 'momentum', 'value')
            top_n: Количество акций
            
        Returns:
            DataFrame с лучшими акциями
        """
        if strategy not in self.strategy_results:
            # Если стратегия еще не рассчитана, рассчитываем
            if strategy == 'rsi':
                self.match_rsi_strategy()
            elif strategy == 'sma':
                self.match_sma_strategy()
            elif strategy == 'momentum':
                self.match_momentum_strategy()
            elif strategy == 'value':
                self.match_value_strategy()
            else:
                logger.error(f"Неизвестная стратегия: {strategy}")
                return pd.DataFrame()
        
        result = self.strategy_results.get(strategy, pd.DataFrame())
        if not result.empty:
            return result.head(top_n)
        return pd.DataFrame()

    def get_all_recommendations(self, min_score: float = 3.0) -> pd.DataFrame:
        """
        Возвращает объединенные рекомендации по всем стратегиям.
        
        Args:
            min_score: Минимальная общая оценка для включения
            
        Returns:
            DataFrame с рекомендациями
        """
        if self.results.empty:
            return pd.DataFrame()
        
        # Рассчитываем все стратегии
        self.match_rsi_strategy()
        self.match_sma_strategy()
        self.match_momentum_strategy()
        self.match_value_strategy()
        
        # Собираем рекомендации
        recommendations = []
        
        for strategy, df in self.strategy_results.items():
            for _, row in df.head(10).iterrows():
                score_col = f"{strategy}_score"
                signal_col = f"{strategy}_signal"
                
                if score_col in row and row[score_col] > 30:  # Порог для рекомендации
                    recommendations.append({
                        'ticker': row['ticker'],
                        'name': row['name'],
                        'strategy': strategy,
                        'score': row[score_col],
                        'signal': row.get(signal_col, ''),
                        'price': row['price'],
                        'rsi': row['rsi']
                    })
        
        if recommendations:
            result = pd.DataFrame(recommendations)
            return result.sort_values('score', ascending=False)
        else:
            return pd.DataFrame()

    def print_strategy_summary(self):
        """Печатает сводку по всем стратегиям."""
        if not self.strategy_results:
            logger.info("Нет рассчитанных стратегий")
            return
        
        print("\n" + "="*80)
        print("📊 СВОДКА ПО СТРАТЕГИЯМ")
        print("="*80)
        
        for strategy_name, df in self.strategy_results.items():
            if df.empty:
                continue
                
            strategy_titles = {
                'rsi': 'RSI Mean Reversion',
                'sma': 'SMA Crossover',
                'momentum': 'Momentum',
                'value': 'Value'
            }
            
            title = strategy_titles.get(strategy_name, strategy_name.upper())
            score_col = f"{strategy_name}_score"
            
            print(f"\n📈 {title}:")
            print("-"*60)
            
            for idx, row in df.head(3).iterrows():
                score = row.get(score_col, 0)
                signal = row.get(f"{strategy_name}_signal", '')
                print(f"  {idx+1}. {row['ticker']:<6} | Оценка: {score:<5.1f} | {signal}")


# Для тестирования
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'ticker': ['SBER', 'GAZP', 'LKOH', 'YDEX', 'PLZL'],
        'name': ['Сбербанк', 'Газпром', 'Лукойл', 'Яндекс', 'Полюс'],
        'price': [250.5, 180.3, 3500.0, 2800.0, 12500.0],
        'rsi': [35, 68, 45, 72, 28],
        'trend': ['up', 'down', 'neutral', 'up', 'up'],
        'bb_position': [0.2, 0.8, 0.5, 0.9, 0.1],
        'volume_ratio': [1.2, 0.8, 1.5, 2.0, 0.9],
        'macd_signal': [1, -1, 0, 1, 1],
        'atr_percent': [1.5, 2.8, 1.2, 3.5, 2.0],
        'score': [75, 45, 60, 55, 85],
        'recommendation': ['ПОКУПКА', 'ИЗБЕГАТЬ', 'НАБЛЮДЕНИЕ', 'НАБЛЮДЕНИЕ', 'СИЛЬНАЯ ПОКУПКА'],
        'sector': ['Finance', 'Energy', 'Energy', 'IT', 'Metals']
    })
    
    print("Тестирование StrategyMatcher...")
    
    matcher = StrategyMatcher(test_data)
    
    # Тест RSI стратегии
    print("\nRSI стратегия:")
    rsi_results = matcher.match_rsi_strategy()
    print(rsi_results[['ticker', 'rsi', 'rsi_score', 'rsi_signal']].head())
    
    # Тест SMA стратегии
    print("\nSMA стратегия:")
    sma_results = matcher.match_sma_strategy()
    print(sma_results[['ticker', 'trend', 'sma_score', 'sma_signal']].head())
    
    # Тест Momentum стратегии
    print("\nMomentum стратегия:")
    momentum_results = matcher.match_momentum_strategy()
    print(momentum_results[['ticker', 'macd_signal', 'volume_ratio', 'momentum_score', 'momentum_signal']].head())
    
    # Тест Value стратегии
    print("\nValue стратегия:")
    value_results = matcher.match_value_strategy()
    print(value_results[['ticker', 'rsi', 'bb_position', 'value_score', 'value_signal']].head())
    
    # Сводка
    matcher.print_strategy_summary()
    
    print("\n✅ Тестирование завершено")