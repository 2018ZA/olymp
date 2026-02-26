"""
Основной торговый движок.
Управляет всеми стратегиями и инструментами.
"""

import time
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

from config.trading_config import TRADING_CONFIG
from config.algo_params import ALGO_PARAMS
from data.moex_client import MoexClient
from execution.order_manager import OrderManager
from core.portfolio import Portfolio
from core.risk_manager import RiskManager
from utils.logger import logger
from utils.time_utils import is_market_open

# Импортируем все стратегии
from strategies import (
    SMACrossover,
    RSIMeanReversion,
    PairTradingStrategy
)

class TradingEngine:
    """Главный движок, координирующий все компоненты"""
    
    def __init__(self):
        self.config = TRADING_CONFIG
        self.algo_params = ALGO_PARAMS
        
        # Компоненты
        self.data_client = MoexClient()
        self.order_manager = OrderManager()
        self.portfolio = Portfolio()
        self.risk_manager = RiskManager(self.config)
        
        # Данные и стратегии
        self.strategies: List[Any] = []
        self.market_data: Dict[str, pd.DataFrame] = {}
        self.pair_data: Dict[str, pd.DataFrame] = {}  # Данные для парных стратегий
        
        # Состояние
        self.running = False
        self.last_update = None
        
        logger.info("Trading Engine инициализирован")
    
    def _init_strategies(self):
        """Создает экземпляры стратегий для всех инструментов"""
        active_strategies = self.algo_params.get('active_strategies', {})
        
        for ticker in self.config['tickers']:
            ticker_strategies = active_strategies.get(ticker, [])
            
            for strategy_name in ticker_strategies:
                try:
                    if strategy_name == 'sma_crossover':
                        params = self.algo_params['sma_crossover'].get(
                            ticker, 
                            self.algo_params['sma_crossover']['__default__']
                        )
                        strategy = SMACrossover(
                            instrument=ticker,
                            params=params,
                            quantity=self.config['quantities'].get(ticker, 1)
                        )
                        self.strategies.append(strategy)
                        logger.info(f"Создана стратегия SMA Crossover для {ticker}")
                    
                    elif strategy_name == 'rsi_mean_reversion':
                        params = self.algo_params['rsi_mean_reversion'].get(
                            ticker,
                            self.algo_params['rsi_mean_reversion']['__default__']
                        )
                        strategy = RSIMeanReversion(
                            instrument=ticker,
                            params=params,
                            quantity=self.config['quantities'].get(ticker, 1)
                        )
                        self.strategies.append(strategy)
                        logger.info(f"Создана стратегия RSI Mean Reversion для {ticker}")
                    
                except Exception as e:
                    logger.error(f"Ошибка создания стратегии {strategy_name} для {ticker}: {e}")
        
        # Создаем парные стратегии отдельно
        if 'pair_trading' in self.algo_params:
            pair_params = self.algo_params['pair_trading']
            for pair in pair_params.get('PAIRS', []):
                try:
                    strategy = PairTradingStrategy(
                        instrument=pair['asset1'],
                        params={
                            'pair_instrument': pair['asset2'],
                            'lookback_period': pair_params.get('lookback_period', 100),
                            'entry_z': pair.get('entry_z', 2.0),
                            'exit_z': pair.get('exit_z', 0.5),
                            'hedge_ratio_update': pair_params.get('hedge_ratio_update', 50)
                        },
                        quantity=self.config['quantities'].get(pair['asset1'], 1)
                    )
                    self.strategies.append(strategy)
                    logger.info(f"Создана стратегия Pair Trading: {pair['asset1']}/{pair['asset2']}")
                    
                except Exception as e:
                    logger.error(f"Ошибка создания парной стратегии: {e}")
        
        logger.info(f"Всего создано стратегий: {len(self.strategies)}")
    
    def _load_initial_data(self):
        """Загружает исторические данные для всех инструментов"""
        all_tickers = self.config['tickers']
        
        # Добавляем инструменты из парных стратегий
        if 'pair_trading' in self.algo_params:
            for pair in self.algo_params['pair_trading'].get('PAIRS', []):
                if pair['asset2'] not in all_tickers:
                    all_tickers.append(pair['asset2'])
        
        # Загружаем данные
        history = self.data_client.get_multiple_history(
            all_tickers, 
            self.config['history_days']
        )
        
        for ticker, df in history.items():
            if not df.empty:
                self.market_data[ticker] = df
                logger.info(f"Загружена история для {ticker}: {len(df)} свечей")
        
        # Передаем данные стратегиям
        for strategy in self.strategies:
            if strategy.instrument in self.market_data:
                strategy.set_initial_data(self.market_data[strategy.instrument])
            
            # Для парных стратегий передаем данные второго инструмента
            if isinstance(strategy, PairTradingStrategy):
                if hasattr(strategy, 'pair_instrument'):
                    pair_ticker = strategy.pair_instrument
                    if pair_ticker in self.market_data:
                        strategy.set_pair_data(self.market_data[pair_ticker])
    
    def _update_market_data(self):
        """Обновляет рыночные данные для всех инструментов"""
        all_tickers = list(self.market_data.keys())
        new_data = self.data_client.get_all_last_candles(
            all_tickers,
            self.config['lookback_bars']
        )
        
        for ticker, df in new_data.items():
            if not df.empty:
                self.market_data[ticker] = df
        
        # Обновляем данные в стратегиях
        for strategy in self.strategies:
            if strategy.instrument in self.market_data:
                strategy.on_data(self.market_data[strategy.instrument])
            
            if isinstance(strategy, PairTradingStrategy):
                if hasattr(strategy, 'pair_instrument'):
                    pair_ticker = strategy.pair_instrument
                    if pair_ticker in self.market_data:
                        strategy.set_pair_data(self.market_data[pair_ticker])
    
    def _process_signals(self):
        """Обрабатывает сигналы от всех стратегий"""
        for strategy in self.strategies:
            try:
                if strategy.has_order_signal():
                    order = strategy.get_order()
                    
                    # Проверяем риск-менеджмент
                    if self.risk_manager.check_order(order):
                        # Отправляем ордер
                        if self.order_manager.send_order(order):
                            # Обновляем портфель
                            self.portfolio.update(order)
                            
                            # Логируем сделку
                            logger.info(f"Сделка исполнена: {order}")
                    
            except Exception as e:
                logger.error(f"Ошибка обработки сигнала от {strategy.name}: {e}")
    
    def start(self):
        """Запускает торговый движок"""
        logger.info("Запуск Trading Engine...")
        
        # Инициализация
        self._init_strategies()
        self._load_initial_data()
        
        self.running = True
        logger.info("Trading Engine запущен")
        
        # Основной цикл
        while self.running:
            try:
                current_time = datetime.now().time()
                
                # Проверяем, открыт ли рынок
                if is_market_open(
                    current_time,
                    self.config['trading_start_time'],
                    self.config['trading_end_time']
                ):
                    # Обновляем данные
                    self._update_market_data()
                    
                    # Обрабатываем сигналы
                    self._process_signals()
                    
                    # Проверяем стоп-лоссы
                    stop_orders = self.portfolio.check_stop_losses(self.market_data)
                    for order in stop_orders:
                        self.order_manager.send_order(order)
                    
                    self.last_update = datetime.now()
                    
                else:
                    # Если рынок закрыт, проверяем не пора ли завершить день
                    end_time = datetime.strptime(
                        self.config['trading_end_time'], 
                        '%H:%M:%S'
                    ).time()
                    
                    if current_time > end_time:
                        logger.info("Торговый день окончен. Закрываем позиции...")
                        self.stop()
                        break
                
                # Ждем следующий цикл
                time.sleep(self.config['fetch_interval'])
                
            except KeyboardInterrupt:
                logger.info("Получен сигнал остановки")
                self.stop()
            except Exception as e:
                logger.error(f"Критическая ошибка в основном цикле: {e}")
                time.sleep(10)
    
    def stop(self):
        """Останавливает движок и закрывает позиции"""
        logger.info("Остановка Trading Engine...")
        
        # Закрываем все позиции
        positions = self.portfolio.get_all_positions()
        if positions:
            self.order_manager.close_all_positions(positions)
        
        # Сбрасываем счетчик ордеров на следующий день
        self.order_manager.reset_daily_counter()
        
        self.running = False
        logger.info("Trading Engine остановлен")