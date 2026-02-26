# screener/ranker.py
"""
Модуль для ранжирования акций на основе различных метрик.
Работает с результатами от обновленного StockScreener.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class Ranker:
    """
    Класс для ранжирования акций по различным критериям.
    Принимает результаты от StockScreener и вычисляет рейтинги.
    """

    def __init__(self, screener_results: pd.DataFrame = None):
        """
        Инициализация ранкера.
        
        Args:
            screener_results: DataFrame с результатами от StockScreener
        """
        self.results = screener_results if screener_results is not None else pd.DataFrame()
        self.rankings = {}

    def set_results(self, screener_results: pd.DataFrame):
        """
        Устанавливает результаты для ранжирования.
        
        Args:
            screener_results: DataFrame с результатами от StockScreener
        """
        self.results = screener_results
        logger.info(f"Загружены результаты для ранжирования: {len(self.results)} акций")

    def rank_by_score(self, ascending: bool = False) -> pd.DataFrame:
        """
        Ранжирует акции по общей оценке (score).
        
        Args:
            ascending: Сортировать по возрастанию (для поиска худших)
            
        Returns:
            DataFrame с добавленным рангом
        """
        if self.results.empty:
            logger.warning("Нет данных для ранжирования")
            return pd.DataFrame()

        df = self.results.copy()
        df['rank_score'] = df['score'].rank(ascending=ascending, method='min')
        df = df.sort_values('rank_score', ascending=ascending)
        
        self.rankings['by_score'] = df
        logger.info(f"Ранжирование по score завершено")
        
        return df

    def rank_by_rsi(self, prefer_oversold: bool = True) -> pd.DataFrame:
        """
        Ранжирует акции по RSI.
        
        Args:
            prefer_oversold: Если True, то выше ранг у перепроданных (низкий RSI),
                            если False, то выше у перекупленных (высокий RSI)
            
        Returns:
            DataFrame с ранжированием по RSI
        """
        if self.results.empty:
            return pd.DataFrame()

        df = self.results.copy()
        
        if prefer_oversold:
            # Чем ниже RSI, тем лучше (перепроданность)
            df['rank_rsi'] = df['rsi'].rank(ascending=True, method='min')
            df['rsi_signal'] = 'Ищем перепроданность'
        else:
            # Чем выше RSI, тем лучше (перекупленность)
            df['rank_rsi'] = df['rsi'].rank(ascending=False, method='min')
            df['rsi_signal'] = 'Ищем перекупленность'
            
        df = df.sort_values('rank_rsi')
        
        self.rankings['by_rsi'] = df
        logger.info(f"Ранжирование по RSI завершено")
        
        return df

    def rank_by_trend(self) -> pd.DataFrame:
        """
        Ранжирует акции по силе тренда.
        
        Returns:
            DataFrame с ранжированием по тренду
        """
        if self.results.empty:
            return pd.DataFrame()

        df = self.results.copy()
        
        # Создаем числовое представление тренда
        trend_map = {'up': 3, 'neutral': 2, 'down': 1}
        df['trend_numeric'] = df['trend'].map(trend_map)
        
        # Ранжируем
        df['rank_trend'] = df['trend_numeric'].rank(ascending=False, method='min')
        df = df.sort_values('rank_trend')
        
        self.rankings['by_trend'] = df
        logger.info(f"Ранжирование по тренду завершено")
        
        return df

    def rank_by_momentum(self) -> pd.DataFrame:
        """
        Ранжирует акции по импульсу (MACD сигнал + объем).
        
        Returns:
            DataFrame с ранжированием по импульсу
        """
        if self.results.empty:
            return pd.DataFrame()

        df = self.results.copy()
        
        # Создаем метрику импульса
        df['momentum_metric'] = 0.0
        
        # MACD сигнал
        df.loc[df['macd_signal'] == 1, 'momentum_metric'] += 2
        df.loc[df['macd_signal'] == -1, 'momentum_metric'] -= 1
        
        # Объем
        df['momentum_metric'] += df['volume_ratio']
        
        # Ранжируем
        df['rank_momentum'] = df['momentum_metric'].rank(ascending=False, method='min')
        df = df.sort_values('rank_momentum')
        
        self.rankings['by_momentum'] = df
        logger.info(f"Ранжирование по импульсу завершено")
        
        return df

    def rank_by_volatility(self, prefer_low: bool = True) -> pd.DataFrame:
        """
        Ранжирует акции по волатильности (ATR%).
        
        Args:
            prefer_low: Если True, то выше ранг у низкой волатильности
            
        Returns:
            DataFrame с ранжированием по волатильности
        """
        if self.results.empty:
            return pd.DataFrame()

        df = self.results.copy()
        
        # Ранжируем по ATR%
        df['rank_volatility'] = df['atr_percent'].rank(ascending=prefer_low, method='min')
        df = df.sort_values('rank_volatility')
        
        self.rankings['by_volatility'] = df
        logger.info(f"Ранжирование по волатильности завершено")
        
        return df

    def rank_by_volume_trend(self) -> pd.DataFrame:
        """
        Ранжирует акции по тренду объема (volume_ratio).
        
        Returns:
            DataFrame с ранжированием по объему
        """
        if self.results.empty:
            return pd.DataFrame()

        df = self.results.copy()
        
        # Чем выше отношение объема к среднему, тем лучше
        df['rank_volume'] = df['volume_ratio'].rank(ascending=False, method='min')
        df = df.sort_values('rank_volume')
        
        self.rankings['by_volume'] = df
        logger.info(f"Ранжирование по объему завершено")
        
        return df

    def rank_by_sector(self, sector: str) -> pd.DataFrame:
        """
        Ранжирует акции внутри конкретного сектора.
        
        Args:
            sector: Название сектора
            
        Returns:
            DataFrame с ранжированием внутри сектора
        """
        if self.results.empty:
            return pd.DataFrame()

        df = self.results.copy()
        
        # Фильтруем по сектору
        sector_df = df[df['sector'] == sector].copy()
        
        if sector_df.empty:
            logger.warning(f"Нет акций в секторе {sector}")
            return pd.DataFrame()
        
        # Ранжируем внутри сектора по score
        sector_df['rank_in_sector'] = sector_df['score'].rank(ascending=False, method='min')
        sector_df = sector_df.sort_values('rank_in_sector')
        
        key = f'sector_{sector}'
        self.rankings[key] = sector_df
        logger.info(f"Ранжирование в секторе {sector} завершено")
        
        return sector_df

    def get_top_by_criteria(self, criteria: str, n: int = 5) -> pd.DataFrame:
        """
        Возвращает топ-N акций по заданному критерию.
        
        Args:
            criteria: Критерий ('score', 'rsi', 'trend', 'momentum', 'volatility', 'volume')
            n: Количество акций
            
        Returns:
            DataFrame с топ-N акциями
        """
        criteria_map = {
            'score': 'by_score',
            'rsi': 'by_rsi',
            'trend': 'by_trend',
            'momentum': 'by_momentum',
            'volatility': 'by_volatility',
            'volume': 'by_volume'
        }
        
        rank_key = criteria_map.get(criteria)
        if not rank_key:
            logger.error(f"Неизвестный критерий: {criteria}")
            return pd.DataFrame()
        
        # Если ранжирование еще не выполнено, выполняем
        if rank_key not in self.rankings:
            if criteria == 'score':
                self.rank_by_score()
            elif criteria == 'rsi':
                self.rank_by_rsi()
            elif criteria == 'trend':
                self.rank_by_trend()
            elif criteria == 'momentum':
                self.rank_by_momentum()
            elif criteria == 'volatility':
                self.rank_by_volatility()
            elif criteria == 'volume':
                self.rank_by_volume_trend()
        
        df = self.rankings.get(rank_key, pd.DataFrame())
        if not df.empty:
            return df.head(n)
        return pd.DataFrame()

    def get_bottom_by_criteria(self, criteria: str, n: int = 5) -> pd.DataFrame:
        """
        Возвращает худшие N акций по заданному критерию.
        
        Args:
            criteria: Критерий ('score', 'rsi', 'trend', 'momentum', 'volatility', 'volume')
            n: Количество акций
            
        Returns:
            DataFrame с худшими акциями
        """
        if criteria == 'score':
            return self.rank_by_score(ascending=True).head(n)
        elif criteria == 'rsi':
            return self.rank_by_rsi(prefer_oversold=False).head(n)
        elif criteria == 'trend':
            df = self.rank_by_trend()
            return df.tail(n).sort_values('rank_trend')
        elif criteria == 'momentum':
            df = self.rank_by_momentum()
            return df.tail(n).sort_values('rank_momentum')
        elif criteria == 'volatility':
            return self.rank_by_volatility(prefer_low=False).head(n)
        elif criteria == 'volume':
            df = self.rank_by_volume_trend()
            return df.tail(n).sort_values('rank_volume')
        else:
            logger.error(f"Неизвестный критерий: {criteria}")
            return pd.DataFrame()

    def get_rank_summary(self) -> pd.DataFrame:
        """
        Возвращает сводный рейтинг по всем критериям.
        
        Returns:
            DataFrame со сводными рангами
        """
        if self.results.empty:
            return pd.DataFrame()

        # Выполняем все ранжирования
        self.rank_by_score()
        self.rank_by_rsi()
        self.rank_by_trend()
        self.rank_by_momentum()
        self.rank_by_volatility()
        self.rank_by_volume_trend()
        
        # Собираем все ранги в одну таблицу
        summary = self.results[['ticker', 'name', 'sector', 'score']].copy()
        
        for key, df in self.rankings.items():
            rank_col = f'rank_{key.replace("by_", "")}'
            if rank_col in df.columns:
                # Добавляем ранг в сводную таблицу
                ticker_ranks = df[['ticker', rank_col]].set_index('ticker')
                summary = summary.join(ticker_ranks, on='ticker')
        
        # Вычисляем средний ранг
        rank_columns = [col for col in summary.columns if col.startswith('rank_')]
        if rank_columns:
            summary['avg_rank'] = summary[rank_columns].mean(axis=1)
            summary = summary.sort_values('avg_rank')
        
        return summary

    def print_top_by_all_criteria(self, n: int = 3):
        """
        Печатает топ-N акций по каждому критерию.
        
        Args:
            n: Количество акций для каждого критерия
        """
        print("\n" + "="*90)
        print("🏆 ТОП АКЦИЙ ПО РАЗНЫМ КРИТЕРИЯМ")
        print("="*90)
        
        criteria_list = [
            ('score', 'Общая оценка'),
            ('rsi', 'RSI (перепроданность)'),
            ('trend', 'Сила тренда'),
            ('momentum', 'Импульс'),
            ('volume', 'Рост объема'),
            ('volatility', 'Низкая волатильность')
        ]
        
        for criteria, title in criteria_list:
            top = self.get_top_by_criteria(criteria, n)
            if not top.empty:
                print(f"\n📊 {title}:")
                print("-"*50)
                
                for idx, row in top.iterrows():
                    value = ""
                    if criteria == 'score':
                        value = f"Оценка: {row['score']:.1f}"
                    elif criteria == 'rsi':
                        value = f"RSI: {row['rsi']:.1f}"
                    elif criteria == 'trend':
                        value = f"Тренд: {row['trend']}"
                    elif criteria == 'momentum':
                        value = f"MACD: {row['macd_signal']}, Объем: {row['volume_ratio']:.1f}"
                    elif criteria == 'volume':
                        value = f"Объем: {row['volume_ratio']:.1f}x"
                    elif criteria == 'volatility':
                        value = f"ATR%: {row['atr_percent']:.1f}%"
                    
                    print(f"  {idx+1}. {row['ticker']:<6} - {row['name'][:25]:<25} | {value}")

    def print_sector_leaders(self):
        """
        Печатает лидеров в каждом секторе.
        """
        if self.results.empty:
            return
        
        print("\n" + "="*90)
        print("🏢 ЛИДЕРЫ ПО СЕКТОРАМ")
        print("="*90)
        
        sectors = self.results['sector'].unique()
        
        for sector in sectors:
            sector_df = self.rank_by_sector(sector)
            if not sector_df.empty:
                print(f"\n📌 {sector}:")
                print("-"*50)
                
                for idx, row in sector_df.head(2).iterrows():
                    print(f"  {int(row['rank_in_sector'])}. {row['ticker']:<6} - {row['name'][:25]:<25} | "
                          f"Оценка: {row['score']:.1f} | {row['recommendation']}")


# Для тестирования
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'ticker': ['SBER', 'GAZP', 'LKOH', 'YDEX', 'PLZL', 'AFLT', 'VTBR', 'NLMK', 'MGNT', 'ROSN'],
        'name': ['Сбербанк', 'Газпром', 'Лукойл', 'Яндекс', 'Полюс', 
                 'Аэрофлот', 'ВТБ', 'НЛМК', 'Магнит', 'Роснефть'],
        'sector': ['Finance', 'Energy', 'Energy', 'IT', 'Metals',
                   'Transport', 'Finance', 'Metals', 'Retail', 'Energy'],
        'price': [250.5, 180.3, 3500.0, 2800.0, 12500.0, 45.6, 0.035, 210.5, 5500.0, 520.0],
        'rsi': [35, 68, 45, 72, 28, 55, 42, 61, 38, 58],
        'trend': ['up', 'down', 'neutral', 'up', 'up', 'down', 'up', 'neutral', 'up', 'neutral'],
        'bb_position': [0.2, 0.8, 0.5, 0.9, 0.1, 0.4, 0.3, 0.7, 0.25, 0.55],
        'volume_ratio': [1.2, 0.8, 1.5, 2.0, 0.9, 1.1, 1.8, 0.7, 1.3, 0.95],
        'macd_signal': [1, -1, 0, 1, 1, -1, 1, 0, 1, 0],
        'atr_percent': [1.5, 2.8, 1.2, 3.5, 2.0, 2.2, 1.8, 1.6, 1.9, 1.4],
        'score': [75, 45, 60, 55, 85, 40, 70, 50, 65, 55],
        'recommendation': ['ПОКУПКА', 'ИЗБЕГАТЬ', 'НАБЛЮДЕНИЕ', 'НАБЛЮДЕНИЕ', 
                          'СИЛЬНАЯ ПОКУПКА', 'ИЗБЕГАТЬ', 'ПОКУПКА', 'НАБЛЮДЕНИЕ', 'ПОКУПКА', 'НАБЛЮДЕНИЕ']
    })
    
    print("Тестирование Ranker...")
    
    ranker = Ranker(test_data)
    
    # Тест различных ранжирований
    print("\n1. Топ по общей оценке:")
    top_score = ranker.get_top_by_criteria('score', 3)
    print(top_score[['ticker', 'score', 'recommendation']].to_string(index=False))
    
    print("\n2. Топ по RSI (перепроданность):")
    top_rsi = ranker.get_top_by_criteria('rsi', 3)
    print(top_rsi[['ticker', 'rsi', 'rsi_signal']].to_string(index=False))
    
    print("\n3. Топ по импульсу:")
    top_momentum = ranker.get_top_by_criteria('momentum', 3)
    print(top_momentum[['ticker', 'macd_signal', 'volume_ratio', 'momentum_metric']].to_string(index=False))
    
    # Сводный рейтинг
    print("\n4. Сводный рейтинг:")
    summary = ranker.get_rank_summary()
    if not summary.empty:
        print(summary[['ticker', 'score', 'avg_rank']].head(5).to_string(index=False))
    
    # Лидеры секторов
    ranker.print_sector_leaders()
    
    # Топ по всем критериям
    ranker.print_top_by_all_criteria()
    
    print("\n✅ Тестирование Ranker завершено")