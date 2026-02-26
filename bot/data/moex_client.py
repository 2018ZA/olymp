# data/moex_client.py
"""
Клиент для получения данных с Московской Биржи (MOEX) через библиотеку moexalgo.
Полностью переработан для использования moexalgo вместо старого API.
"""

import pandas as pd
from moexalgo import Ticker, Market
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import logging

logger = logging.getLogger(__name__)

class MoexClient:
    """
    Клиент для взаимодействия с MOEX ALGOPACK через библиотеку moexalgo.
    Предоставляет методы для получения свечей, стаканов и другой рыночной информации.
    """

    def __init__(self):
        """Инициализация клиента."""
        # Требуется Python >= 3.12 и установка moexalgo[dataframe]
        pass

    def get_candles(self, ticker: str, days: int = 3, interval: int = 60) -> pd.DataFrame:
        """
        Получает свечи по тикеру за последние N дней.
        
        Args:
            ticker: Тикер инструмента (например, 'SBER')
            days: Количество дней истории для загрузки
            interval: Интервал свечи в минутах (1, 10, 60)
            
        Returns:
            DataFrame со свечами и колонками: 'begin', 'open', 'close', 'high', 'low', 'volume'
        """
        try:
            t = Ticker(ticker)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')

            # Получаем данные
            candles_data = t.candles(start=start_str, end=end_str)

            # Преобразуем в DataFrame если это генератор
            if isinstance(candles_data, pd.DataFrame):
                df = candles_data
            else:
                df = pd.DataFrame(candles_data)

            if df.empty:
                logger.warning(f"Нет данных по свечам для {ticker} за период {start_str} - {end_str}")
                return pd.DataFrame(columns=['begin', 'open', 'close', 'high', 'low', 'volume'])

            # Стандартизируем названия колонок
            column_mapping = {
                'OPEN': 'open', 'open': 'open',
                'CLOSE': 'close', 'close': 'close',
                'HIGH': 'high', 'high': 'high',
                'LOW': 'low', 'low': 'low',
                'VOLUME': 'volume', 'volume': 'volume',
                'BEGIN': 'begin', 'begin': 'begin',
                'END': 'end', 'end': 'end'
            }
            df = df.rename(columns={col: column_mapping.get(col, col) for col in df.columns if col in column_mapping})

            # Проверяем наличие обязательных колонок
            required_cols = ['begin', 'open', 'close', 'high', 'low', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"В данных для {ticker} отсутствуют колонки: {missing_cols}")
                return pd.DataFrame(columns=required_cols)

            # Оставляем только нужные колонки и сортируем по времени
            df = df[required_cols].copy()
            df['begin'] = pd.to_datetime(df['begin'])
            df = df.sort_values('begin').reset_index(drop=True)

            # Конвертируем все в float для безопасности
            for col in ['open', 'close', 'high', 'low', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.debug(f"Загружено {len(df)} свечей для {ticker}")
            return df

        except Exception as e:
            logger.error(f"Ошибка получения свечей для {ticker}: {e}")
            return pd.DataFrame(columns=['begin', 'open', 'close', 'high', 'low', 'volume'])

    def get_current_price(self, ticker: str) -> Optional[float]:
        """
        Получает текущую цену последней сделки по тикеру.
        
        Args:
            ticker: Тикер инструмента
            
        Returns:
            Текущая цена или None в случае ошибки
        """
        try:
            t = Ticker(ticker)
            
            # Пробуем получить последнюю свечу
            today_str = datetime.now().strftime('%Y-%m-%d')
            candles = t.candles(start=today_str, end=today_str)
            
            if isinstance(candles, pd.DataFrame) and not candles.empty:
                # Берем цену закрытия последней свечи
                price = float(candles['close'].iloc[-1])
                logger.debug(f"Текущая цена {ticker}: {price} (из свечей)")
                return price
            
            # Если свечей нет, пробуем получить из стакана
            ob = t.orderbook()
            if isinstance(ob, pd.DataFrame) and not ob.empty:
                bids = ob[ob['side'] == 'Bid']
                asks = ob[ob['side'] == 'Ask']
                if not bids.empty and not asks.empty:
                    best_bid = float(bids['price'].max())
                    best_ask = float(asks['price'].min())
                    mid_price = (best_bid + best_ask) / 2
                    logger.debug(f"Текущая цена {ticker}: {mid_price} (из стакана)")
                    return mid_price
            
            logger.warning(f"Не удалось получить цену для {ticker}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения цены для {ticker}: {e}")
            return None

    def get_orderbook(self, ticker: str, depth: int = 10) -> Dict[str, pd.DataFrame]:
        """
        Получает стакан котировок.
        
        Args:
            ticker: Тикер инструмента
            depth: Глубина стакана (количество уровней)
            
        Returns:
            Словарь с bid и ask DataFrame'ами (колонки: price, volume)
        """
        try:
            t = Ticker(ticker)
            ob_data = t.orderbook()

            if isinstance(ob_data, pd.DataFrame) and not ob_data.empty:
                bids = ob_data[ob_data['side'] == 'Bid'][['price', 'volume']].head(depth)
                asks = ob_data[ob_data['side'] == 'Ask'][['price', 'volume']].head(depth)
                
                # Конвертируем в float где нужно
                bids['price'] = pd.to_numeric(bids['price'], errors='coerce')
                bids['volume'] = pd.to_numeric(bids['volume'], errors='coerce')
                asks['price'] = pd.to_numeric(asks['price'], errors='coerce')
                asks['volume'] = pd.to_numeric(asks['volume'], errors='coerce')
                
                return {
                    'bids': bids.reset_index(drop=True),
                    'asks': asks.reset_index(drop=True)
                }
            else:
                return {'bids': pd.DataFrame(), 'asks': pd.DataFrame()}
                
        except Exception as e:
            logger.error(f"Ошибка получения стакана для {ticker}: {e}")
            return {'bids': pd.DataFrame(), 'asks': pd.DataFrame()}

    def get_tickers_list(self, market: str = 'stocks') -> List[str]:
        """
        Возвращает список всех доступных тикеров на указанном рынке.
        В moexalgo нет прямого метода, поэтому возвращаем список из конфига.
        
        Args:
            market: Рынок ('stocks', 'futures', 'currency')
            
        Returns:
            Список тикеров
        """
        # Пока возвращаем пустой список, так как в проекте тикеры берутся из конфига
        # В будущем можно добавить парсинг с сайта MOEX
        logger.warning("Метод get_tickers_list не полностью реализован в moexalgo")
        return []

    def get_ticker_info(self, ticker: str) -> Optional[Dict]:
        """
        Получает основную информацию о тикере.
        В moexalgo нет прямого метода, возвращаем базовую информацию.
        
        Args:
            ticker: Тикер инструмента
            
        Returns:
            Словарь с информацией о тикере или None
        """
        try:
            # Базовая информация о тикере
            # В реальном проекте можно добавить загрузку из CSV файла со справочником
            sector_map = {
                'SBER': 'Finance',
                'GAZP': 'Energy',
                'LKOH': 'Energy',
                'ROSN': 'Energy',
                'YDEX': 'IT',
                'PLZL': 'Metals',
                'NLMK': 'Metals',
                'MGNT': 'Retail',
                'AFLT': 'Transport',
                'VTBR': 'Finance'
            }
            
            name_map = {
                'SBER': 'Сбербанк',
                'GAZP': 'Газпром',
                'LKOH': 'Лукойл',
                'ROSN': 'Роснефть',
                'YDEX': 'Яндекс',
                'PLZL': 'Полюс',
                'NLMK': 'НЛМК',
                'MGNT': 'Магнит',
                'AFLT': 'Аэрофлот',
                'VTBR': 'ВТБ'
            }
            
            return {
                'ticker': ticker,
                'name': name_map.get(ticker, ticker),
                'sector': sector_map.get(ticker, 'unknown'),
                'lot_size': 1
            }
            
        except Exception as e:
            logger.error(f"Ошибка получения информации для {ticker}: {e}")
            return {
                'ticker': ticker,
                'name': ticker,
                'sector': 'unknown',
                'lot_size': 1
            }


# Для тестирования
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    client = MoexClient()
    
    # Тест 1: Получение свечей
    ticker = "SBER"
    print(f"\n{'='*50}")
    print(f"ТЕСТИРОВАНИЕ КЛИЕНТА MOEXALGO")
    print(f"{'='*50}")
    
    print(f"\n1. Получение свечей для {ticker} за 3 дня:")
    candles = client.get_candles(ticker, days=3)
    if not candles.empty:
        print(f"   ✅ Загружено {len(candles)} свечей")
        print(f"   Первые 3 записи:")
        print(candles[['begin', 'open', 'close', 'volume']].head(3).to_string(index=False))
    else:
        print(f"   ❌ Не удалось загрузить свечи")
    
    # Тест 2: Текущая цена
    print(f"\n2. Текущая цена {ticker}:")
    price = client.get_current_price(ticker)
    if price:
        print(f"   ✅ {price:.2f} RUB")
    else:
        print(f"   ❌ Не удалось получить цену")
    
    # Тест 3: Информация о тикере
    print(f"\n3. Информация о {ticker}:")
    info = client.get_ticker_info(ticker)
    if info:
        print(f"   Название: {info['name']}")
        print(f"   Сектор: {info['sector']}")