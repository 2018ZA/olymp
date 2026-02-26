"""
Параметры торговых алгоритмов для разных инструментов.
"""

ALGO_PARAMS = {
    # Стратегия 1: Пересечение скользящих средних
    'sma_crossover': {
        'GAZP': {'sma_fast': 5, 'sma_slow': 15},
        'SBER': {'sma_fast': 7, 'sma_slow': 20},
        'LKOH': {'sma_fast': 5, 'sma_slow': 15},
        'PLZL': {'sma_fast': 8, 'sma_slow': 21},
        # Значения по умолчанию для всех остальных
        '__default__': {'sma_fast': 5, 'sma_slow': 15}
    },
    
    # Стратегия 2: RSI Mean Reversion
    'rsi_mean_reversion': {
        'GAZP': {'rsi_period': 14, 'oversold': 30, 'overbought': 70},
        'SBER': {'rsi_period': 14, 'oversold': 25, 'overbought': 75},
        'LKOH': {'rsi_period': 14, 'oversold': 30, 'overbought': 70},
        '__default__': {'rsi_period': 14, 'oversold': 30, 'overbought': 70}
    },
    
    # Стратегия 3: Парный трейдинг
    'pair_trading': {
        'PAIRS': [
            {'asset1': 'SBER', 'asset2': 'SBERP', 'entry_z': 2.0, 'exit_z': 0.5},
            {'asset1': 'GAZP', 'asset2': 'LKOH', 'entry_z': 2.0, 'exit_z': 0.5},
            {'asset1': 'PLZL', 'asset2': 'GLH6', 'entry_z': 2.0, 'exit_z': 0.5}
        ],
        'lookback_period': 100,       # Период для расчета спреда
        'hedge_ratio_update': 50       # Как часто пересчитывать коэффициент хеджа
    },
    
    # Какие стратегии активны для каких инструментов
    'active_strategies': {
        'GAZP': ['sma_crossover', 'rsi_mean_reversion'],
        'SBER': ['sma_crossover', 'pair_trading'],
        'LKOH': ['sma_crossover'],
        'PLZL': ['rsi_mean_reversion', 'pair_trading']
    }
}