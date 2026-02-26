"""
Модуль для подбора акций под конкретные торговые стратегии.
"""

from typing import List, Dict, Any
from .stock_screener import StockScore


class StrategyMatcher:
    """
    Подбирает акции, подходящие для различных торговых стратегий.
    """
    
    @staticmethod
    def get_for_sma_crossover(scores: List[StockScore], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Отбирает акции для стратегии SMA Crossover (трендовая).
        
        Критерии:
        - Цена выше SMA 50 (восходящий тренд)
        - Хороший объем
        - RSI не в зоне перекупленности
        """
        results = []
        
        for score in scores:
            if score.price > score.sma_50 and score.volume_score > 0.5 and score.rsi < 65:
                results.append({
                    'ticker': score.ticker,
                    'name': score.name,
                    'price': score.price,
                    'sma_50': score.sma_50,
                    'rsi': score.rsi,
                    'trend': score.trend_long,
                    'score': score.total_score
                })
        
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:top_n]
    
    @staticmethod
    def get_for_rsi_mean_reversion(scores: List[StockScore], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Отбирает акции для стратегии RSI Mean Reversion.
        
        Критерии:
        - RSI в зоне перепроданности (<35)
        - Цена около нижней полосы Боллинджера
        - Низкая волатильность
        """
        results = []
        
        for score in scores:
            if score.rsi < 35 and score.bb_position < 0.3 and score.atr_percent < 3.0:
                results.append({
                    'ticker': score.ticker,
                    'name': score.name,
                    'price': score.price,
                    'rsi': score.rsi,
                    'bb_position': score.bb_position,
                    'atr_percent': score.atr_percent,
                    'score': score.total_score
                })
        
        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:top_n]
    
    @staticmethod
    def get_for_pair_trading(scores: List[StockScore], all_scores: List[StockScore] = None) -> List[Dict[str, Any]]:
        """
        Находит потенциальные пары для парного трейдинга.
        
        Критерии:
        - Акции из одного сектора
        - Высокая корреляция
        - Текущее расхождение в ценах
        """
        if all_scores is None:
            return []
        
        # Группируем по секторам
        sectors = {}
        for score in all_scores:
            if score.sector not in sectors:
                sectors[score.sector] = []
            sectors[score.sector].append(score)
        
        # Ищем пары внутри секторов
        pairs = []
        for sector, stocks in sectors.items():
            if len(stocks) < 2:
                continue
            
            # Простейший отбор - берем две самые разные по RSI
            stocks_by_rsi = sorted(stocks, key=lambda x: x.rsi)
            if len(stocks_by_rsi) >= 2:
                pairs.append({
                    'sector': sector,
                    'pair': (stocks_by_rsi[0].ticker, stocks_by_rsi[-1].ticker),
                    'rsi_spread': stocks_by_rsi[-1].rsi - stocks_by_rsi[0].rsi
                })
        
        return pairs[:5]
    
    @staticmethod
    def get_for_momentum(scores: List[StockScore], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Отбирает акции для моментной стратегии.
        
        Критерии:
        - Сильный восходящий тренд
        - Высокий momentum_score
        - Растущие объемы
        """
        results = []
        
        for score in scores:
            if score.momentum_score > 0.5 and score.trend_long == 'up' and score.volume_score > 0.7:
                results.append({
                    'ticker': score.ticker,
                    'name': score.name,
                    'price': score.price,
                    'momentum': score.momentum_score,
                    'trend': score.trend_long,
                    'rsi': score.rsi,
                    'score': score.total_score
                })
        
        results.sort(key=lambda x: x['momentum'], reverse=True)
        return results[:top_n]
    
    @staticmethod
    def get_for_value(scores: List[StockScore], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Отбирает акции для стоимостной стратегии.
        
        Критерии:
        - Низкая волатильность
        - Стабильный нисходящий тренд (возможность входа на дне)
        - Низкий RSI
        """
        results = []
        
        for score in scores:
            if score.rsi < 40 and score.atr_percent < 2.0 and score.trend_long == 'down':
                results.append({
                    'ticker': score.ticker,
                    'name': score.name,
                    'price': score.price,
                    'rsi': score.rsi,
                    'atr_percent': score.atr_percent,
                    'trend': score.trend_long,
                    'score': score.total_score
                })
        
        results.sort(key=lambda x: x['rsi'])
        return results[:top_n]