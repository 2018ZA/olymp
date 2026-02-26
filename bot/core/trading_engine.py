# core/trading_engine.py
"""
Основной торговый движок, управляющий стратегиями и исполнением ордеров.
Адаптирован для работы с новым MoexClient на базе moexalgo.
"""

import time
import threading
from datetime import datetime, time as dt_time, timedelta
from typing import Dict, List, Optional
import pandas as pd
import logging

from data.moex_client import MoexClient
from strategies.sma_strategy import SMACrossoverStrategy
from strategies.rsi_mean_reversion import RSIMeanReversionStrategy
from strategies.pair_trading import PairTradingStrategy
from execution.order_manager import OrderManager
from core.portfolio import Portfolio
from core.risk_manager import RiskManager
from config.trading_config import TRADING_CONFIG
from config.algo_params import ALGO_PARAMS

logger = logging.getLogger(__name__)

class TradingEngine:
    """
    Главный движок торговой системы.
    Координирует получение данных, генерацию сигналов и исполнение сделок.
    """

    def __init__(self):
        """Инициализация торгового движка."""
        self.config = TRADING_CONFIG
        self.algo_params = ALGO_PARAMS
        
        # Инициализация компонентов
        self.moex_client = MoexClient()
        self.order_manager = OrderManager()
        self.portfolio = Portfolio()
        self.risk_manager = RiskManager(self.portfolio)
        
        # Словари для хранения стратегий
        self.strategies: Dict[str, List] = {}
        self.strategy_instances: Dict[str, object] = {}
        
        # Данные по инструментам
        self.market_data: Dict[str, pd.DataFrame] = {}
        self.last_prices: Dict[str, float] = {}
        
        # Флаги состояния
        self.is_running = False
        self.trading_active = False
        
        # Статистика
        self.daily_trades = 0
        self.last_check_time = None
        
        logger.info("Trading Engine инициализирован")

    def initialize_strategies(self):
        """
        Инициализирует стратегии для каждого тикера согласно конфигурации.
        """
        tickers = self.config['tickers']
        
        # Загружаем исторические данные для всех тикеров
        logger.info("Загрузка исторических данных для инициализации стратегий...")
        for ticker in tickers:
            data = self.moex_client.get_candles(ticker, days=self.config['history_days'])
            if not data.empty:
                self.market_data[ticker] = data
                logger.info(f"Загружена история для {ticker}: {len(data)} свечей")
            else:
                logger.warning(f"Не удалось загрузить историю для {ticker}")

        # Создаем экземпляры стратегий для каждого тикера
        self.strategy_instances['sma_crossover'] = {}
        self.strategy_instances['rsi_mean_reversion'] = {}
        
        # SMA Crossover стратегия
        sma_params = self.algo_params.get('sma_crossover', {})
        default_sma = sma_params.get('__default__', {'sma_fast': 5, 'sma_slow': 15})
        
        for ticker in tickers:
            params = sma_params.get(ticker, default_sma)
            strategy = SMACrossoverStrategy(
                ticker=ticker,
                sma_fast=params['sma_fast'],
                sma_slow=params['sma_slow']
            )
            
            # Передаем исторические данные в стратегию
            if ticker in self.market_data:
                strategy.set_initial_data(self.market_data[ticker])
            
            self.strategy_instances['sma_crossover'][ticker] = strategy
            logger.info(f"Создана стратегия SMA Crossover для {ticker} (fast={params['sma_fast']}, slow={params['sma_slow']})")
        
        # RSI Mean Reversion стратегия
        rsi_params = self.algo_params.get('rsi_mean_reversion', {})
        default_rsi = rsi_params.get('__default__', {'rsi_period': 14, 'oversold': 30, 'overbought': 70})
        
        for ticker in tickers:
            params = rsi_params.get(ticker, default_rsi)
            strategy = RSIMeanReversionStrategy(
                ticker=ticker,
                rsi_period=params['rsi_period'],
                oversold=params['oversold'],
                overbought=params['overbought']
            )
            
            if ticker in self.market_data:
                strategy.set_initial_data(self.market_data[ticker])
            
            self.strategy_instances['rsi_mean_reversion'][ticker] = strategy
            logger.info(f"Создана стратегия RSI Mean Reversion для {ticker}")
        
        # Pair Trading стратегия (если есть пары)
        pair_params = self.algo_params.get('pair_trading', {})
        if 'PAIRS' in pair_params and pair_params['PAIRS']:
            self.strategy_instances['pair_trading'] = []
            for pair in pair_params['PAIRS']:
                strategy = PairTradingStrategy(
                    asset1=pair['asset1'],
                    asset2=pair['asset2'],
                    entry_z=pair['entry_z']
                )
                # Загружаем данные для обоих активов
                data1 = self.market_data.get(pair['asset1'])
                data2 = self.market_data.get(pair['asset2'])
                if data1 is not None and data2 is not None:
                    strategy.set_initial_data(data1, data2)
                self.strategy_instances['pair_trading'].append(strategy)
                logger.info(f"Создана стратегия Pair Trading для {pair['asset1']}/{pair['asset2']}")

    def start(self):
        """
        Запускает основной цикл торговли.
        """
        self.is_running = True
        logger.info("Trading Engine запущен")
        
        # Основной цикл
        while self.is_running:
            try:
                current_time = datetime.now()
                
                # Проверяем, активно ли торговое время
                self.trading_active = self._is_trading_time(current_time)
                
                if self.trading_active:
                    self._trading_iteration()
                else:
                    # Если рынок закрыт, просто ждем
                    logger.debug("Рынок закрыт, ожидание...")
                
                # Проверяем время закрытия позиций
                if self._is_closing_time(current_time):
                    self._close_all_positions()
                
                # Ждем до следующей итерации
                time.sleep(self.config['fetch_interval'])
                
            except KeyboardInterrupt:
                logger.info("Получен сигнал остановки")
                self.stop()
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}", exc_info=True)
                time.sleep(5)  # Пауза перед повторной попыткой

    def _trading_iteration(self):
        """
        Одна итерация торгового цикла.
        """
        logger.debug("Начало торговой итерации")
        
        # Получаем текущие цены по всем инструментам
        for ticker in self.config['tickers']:
            price = self.moex_client.get_current_price(ticker)
            if price:
                self.last_prices[ticker] = price
                logger.debug(f"Цена {ticker}: {price}")
        
        # Обновляем данные в стратегиях (если нужно)
        self._update_strategies_data()
        
        # Проверяем сигналы SMA стратегий
        for ticker, strategy in self.strategy_instances['sma_crossover'].items():
            if ticker in self.last_prices:
                signal = strategy.generate_signal()
                if signal and self.risk_manager.can_trade(ticker, signal):
                    self._execute_signal(strategy, signal)
        
        # Проверяем сигналы RSI стратегий
        for ticker, strategy in self.strategy_instances['rsi_mean_reversion'].items():
            if ticker in self.last_prices:
                signal = strategy.generate_signal()
                if signal and self.risk_manager.can_trade(ticker, signal):
                    self._execute_signal(strategy, signal)
        
        # Проверяем парные стратегии
        if 'pair_trading' in self.strategy_instances:
            for strategy in self.strategy_instances['pair_trading']:
                signal = strategy.generate_signal()
                if signal and signal['action'] in ['buy_asset1', 'buy_asset2']:
                    # Для парных стратегий нужна особая обработка
                    self._execute_pair_signal(strategy, signal)
        
        # Проверяем стоп-лоссы
        self._check_stop_losses()
        
        # Проверяем дневной лимит сделок
        if self.daily_trades >= self.config['max_daily_trades']:
            logger.warning(f"Достигнут дневной лимит сделок ({self.daily_trades})")
            self.trading_active = False

    def _update_strategies_data(self):
        """
        Обновляет данные в стратегиях новыми свечами.
        В реальном времени обычно используем текущие цены,
        но для расчета индикаторов нужно периодически обновлять свечи.
        """
        # Обновляем свечи каждые N итераций (например, раз в час)
        # В упрощенном варианте пропускаем для экономии запросов
        pass

    def _execute_signal(self, strategy, signal: str):
        """
        Исполняет торговый сигнал.
        
        Args:
            strategy: Экземпляр стратегии
            signal: Сигнал ('buy' или 'sell')
        """
        ticker = strategy.ticker
        quantity = self.config['quantities'].get(ticker, 1)
        price = self.last_prices.get(ticker)
        
        if not price:
            logger.warning(f"Нет цены для {ticker}, сигнал не исполнен")
            return
        
        # Проверяем риск-менеджер
        if not self.risk_manager.check_order(ticker, signal, quantity, price):
            logger.info(f"Риск-менеджер отклонил сделку: {ticker} {signal}")
            return
        
        # Отправляем ордер
        order = self.order_manager.create_order(ticker, signal, quantity, price)
        if order:
            self.daily_trades += 1
            # Обновляем портфель (в реальном приложении - по подтверждению от брокера)
            self.portfolio.update_position(ticker, signal, quantity, price)
            logger.info(f"Сигнал исполнен: {ticker} {signal} {quantity} @ {price:.2f}")
        else:
            logger.error(f"Не удалось создать ордер для {ticker}")

    def _execute_pair_signal(self, strategy, signal: dict):
        """
        Исполняет сигнал от парной стратегии.
        
        Args:
            strategy: Экземпляр парной стратегии
            signal: Словарь с сигналом
        """
        asset1 = strategy.asset1
        asset2 = strategy.asset2
        action = signal['action']
        
        price1 = self.last_prices.get(asset1)
        price2 = self.last_prices.get(asset2)
        
        if not price1 or not price2:
            logger.warning(f"Нет цен для парной сделки {asset1}/{asset2}")
            return
        
        if action == 'buy_asset1':
            # Покупаем asset1, продаем asset2
            qty1 = self.config['quantities'].get(asset1, 1)
            qty2 = self.config['quantities'].get(asset2, 1)
            
            if self.risk_manager.can_trade(asset1, 'buy') and self.risk_manager.can_trade(asset2, 'sell'):
                self.order_manager.create_order(asset1, 'buy', qty1, price1)
                self.order_manager.create_order(asset2, 'sell', qty2, price2)
                logger.info(f"Парная сделка: buy {asset1} / sell {asset2}")
                self.daily_trades += 2
        elif action == 'buy_asset2':
            # Покупаем asset2, продаем asset1
            qty1 = self.config['quantities'].get(asset1, 1)
            qty2 = self.config['quantities'].get(asset2, 1)
            
            if self.risk_manager.can_trade(asset2, 'buy') and self.risk_manager.can_trade(asset1, 'sell'):
                self.order_manager.create_order(asset2, 'buy', qty2, price2)
                self.order_manager.create_order(asset1, 'sell', qty1, price1)
                logger.info(f"Парная сделка: buy {asset2} / sell {asset1}")
                self.daily_trades += 2

    def _check_stop_losses(self):
        """
        Проверяет и исполняет стоп-лоссы.
        """
        for ticker, position in self.portfolio.positions.items():
            if position.quantity == 0:
                continue
            
            current_price = self.last_prices.get(ticker)
            if not current_price:
                continue
            
            # Проверяем стоп-лосс
            stop_loss = self.risk_manager.get_stop_loss(ticker, position)
            if stop_loss and ((position.quantity > 0 and current_price <= stop_loss) or
                             (position.quantity < 0 and current_price >= stop_loss)):
                logger.warning(f"Сработал стоп-лосс для {ticker} по цене {current_price}")
                # Закрываем позицию
                signal = 'sell' if position.quantity > 0 else 'buy'
                quantity = abs(position.quantity)
                self.order_manager.create_order(ticker, signal, quantity, current_price)
                self.portfolio.close_position(ticker)

    def _close_all_positions(self):
        """
        Закрывает все открытые позиции в конце дня.
        """
        logger.info("Закрытие всех позиций...")
        for ticker, position in self.portfolio.positions.items():
            if position.quantity != 0:
                current_price = self.last_prices.get(ticker)
                if current_price:
                    signal = 'sell' if position.quantity > 0 else 'buy'
                    quantity = abs(position.quantity)
                    self.order_manager.create_order(ticker, signal, quantity, current_price)
                    self.portfolio.close_position(ticker)
                    logger.info(f"Закрыта позиция {ticker}: {position.quantity} @ {current_price}")
        
        # Сбрасываем счетчик сделок
        self.daily_trades = 0
        logger.info("Все позиции закрыты")

    def _is_trading_time(self, current_time: datetime) -> bool:
        """
        Проверяет, является ли текущее время торговым.
        
        Args:
            current_time: Текущее время
            
        Returns:
            True если время торговое
        """
        start_time = dt_time.fromisoformat(self.config['trading_start_time'])
        end_time = dt_time.fromisoformat(self.config['trading_end_time'])
        current = current_time.time()
        
        return start_time <= current <= end_time

    def _is_closing_time(self, current_time: datetime) -> bool:
        """
        Проверяет, пришло ли время закрытия позиций.
        
        Args:
            current_time: Текущее время
            
        Returns:
            True если время закрытия
        """
        end_time = dt_time.fromisoformat(self.config['trading_end_time'])
        # Закрываем за 5 минут до конца торгов
        close_time = (datetime.combine(current_time.date(), end_time) - timedelta(minutes=5)).time()
        current = current_time.time()
        
        return current >= close_time and current <= end_time

    def stop(self):
        """
        Останавливает торговый движок.
        """
        self.is_running = False
        logger.info("Trading Engine остановлен")

    def get_status(self) -> dict:
        """
        Возвращает статус торгового движка.
        
        Returns:
            Словарь со статусом
        """
        return {
            'running': self.is_running,
            'trading_active': self.trading_active,
            'daily_trades': self.daily_trades,
            'max_daily_trades': self.config['max_daily_trades'],
            'positions': {k: v.to_dict() for k, v in self.portfolio.positions.items()},
            'last_prices': self.last_prices
        }


if __name__ == "__main__":
    # Для тестирования
    logging.basicConfig(level=logging.INFO)
    engine = TradingEngine()
    engine.initialize_strategies()
    
    # Запускаем в отдельном потоке для демонстрации
    import threading
    thread = threading.Thread(target=engine.start)
    thread.daemon = True
    thread.start()
    
    # Даем поработать 10 секунд
    time.sleep(10)
    engine.stop()
    
    print("\nСтатус движка:", engine.get_status())