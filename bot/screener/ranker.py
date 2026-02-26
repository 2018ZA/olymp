"""
Модуль для ранжирования акций по различным критериям.
"""

from typing import List, Dict, Any, Callable
from .stock_screener import StockScore


class Ranker:
    """
    Класс для ранжирования акций по разным метрикам.
    """
    
    @staticmethod
    def rank_by_rsi(scores: List[StockScore], ascending: bool = True) -> List[Dict[str, Any]]:
        """
        Ранжирует акции по RSI.
        
        Args:
            scores: Список оценок
            ascending: True - от перепроданных к перекупленным
            
        Returns:
            Отранжированный список
        """
        sorted_scores = sorted(scores, key=lambda x: x.rsi, reverse=not ascending)
        
        result = []
        for score in sorted_scores:
            result.append({
                'ticker': score.ticker,
                'name': score.name,
                'rsi': score.rsi,
                'signal': 'oversold' if score.rsi < 30 else ('overbought' if score.rsi > 70 else 'neutral'),
                'price': score.price
            })
        
        return result
    
    @staticmethod
    def rank_by_momentum(scores: List[StockScore]) -> List[Dict[str, Any]]:
        """
        Ранжирует акции по моментуму (силе движения).
        """
        sorted_scores = sorted(scores, key=lambda x: x.momentum_score, reverse=True)
        
        result = []
        for score in sorted_scores:
            result.append({
                'ticker': score.ticker,
                'name': score.name,
                'momentum': score.momentum_score,
                'trend': score.trend_short,
                'price': score.price,
                'score': score.total_score
            })
        
        return result
    
    @staticmethod
    def rank_by_volatility(scores: List[StockScore], low_first: bool = True) -> List[Dict[str, Any]]:
        """
        Ранжирует акции по волатильности.
        
        Args:
            scores: Список оценок
            low_first: True - сначала низковолатильные
        """
        sorted_scores = sorted(scores, key=lambda x: x.atr_percent, reverse=not low_first)
        
        result = []
        for score in sorted_scores:
            result.append({
                'ticker': score.ticker,
                'name': score.name,
                'atr_percent': score.atr_percent,
                'volatility': 'low' if score.atr_percent < 1.5 else ('medium' if score.atr_percent < 2.5 else 'high'),
                'price': score.price
            })
        
        return result
    
    @staticmethod
    def rank_by_bb_position(scores: List[StockScore], near_lower: bool = True) -> List[Dict[str, Any]]:
        """
        Ранжирует акции по позиции в полосах Боллинджера.
        
        Args:
            scores: Список оценок
            near_lower: True - сначала ближе к нижней полосе
        """
        if near_lower:
            sorted_scores = sorted(scores, key=lambda x: x.bb_position)
        else:
            sorted_scores = sorted(scores, key=lambda x: x.bb_position, reverse=True)
        
        result = []
        for score in sorted_scores:
            result.append({
                'ticker': score.ticker,
                'name': score.name,
                'bb_position': score.bb_position,
                'position': 'lower' if score.bb_position < 0.2 else ('upper' if score.bb_position > 0.8 else 'middle'),
                'price': score.price
            })
        
        return result
    
    @staticmethod
    def rank_by_total_score(scores: List[StockScore]) -> List[Dict[str, Any]]:
        """
        Ранжирует акции по итоговой оценке.
        """
        sorted_scores = sorted(scores, key=lambda x: x.total_score, reverse=True)
        
        result = []
        for score in sorted_scores:
            result.append({
                'ticker': score.ticker,
                'name': score.name,
                'total_score': score.total_score,
                'recommendation': score.recommendation,
                'price': score.price,
                'rsi': score.rsi,
                'trend': score.trend_long
            })
        
        return result