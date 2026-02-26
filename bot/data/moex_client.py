"""
Клиент для получения данных с MOEX API.
Поддержка множественных запросов для нескольких инструментов.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from utils.logger import logger
from utils.validators import validate_market_data

class MoexClient:
    """Клиент для работы с MOEX ISS API"""
    
    def __init__(self):
        self.base_url = 'https://iss.moex.com/iss'
        self.cache = {}  # Кэш для хранения последних данных
        
    def get_history(self, ticker: str, days: int) -> pd.DataFrame:
        """
        Получает исторические данные для одного инструмента.
        
        Args:
            ticker: Тикер инструмента (например, 'GAZP')
            days: Количество дней истории
            
        Returns:
            DataFrame с историческими данными
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Формируем URL для MOEX ISS API
            url = f"{self.base_url}/engines/stock/markets/shares/securities/{ticker}/candles.json"
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'till': end_date.strftime('%Y-%m-%d'),
                'interval': 24,  # 24 = daily candles (более стабильные)
                'start': 0,
                'limit': 100
            }
            
            logger.info(f"Запрашиваем данные для {ticker}: {url}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # MOEX API возвращает данные в формате: {"candles": {"columns": [...], "data": [...]}}
            if 'candles' not in data:
                logger.warning(f"Нет данных свечей для {ticker}")
                return pd.DataFrame()
            
            candles_data = data['candles']
            
            # Получаем названия колонок из ответа API
            columns = candles_data.get('columns', [])
            candles = candles_data.get('data', [])
            
            if not candles:
                logger.warning(f"Пустой список свечей для {ticker}")
                return pd.DataFrame()
            
            logger.info(f"Получено {len(candles)} свечей для {ticker}")
            logger.info(f"Колонки от API: {columns}")
            
            # Создаем DataFrame с правильными колонками
            df = pd.DataFrame(candles, columns=columns)
            
            # Переименовываем колонки в наш стандарт
            column_mapping = {
                'open': 'open',
                'high': 'high', 
                'low': 'low',
                'close': 'close',
                'volume': 'volume',
                'begin': 'time',  # или 'end' - смотрим, что приходит
            }
            
            # Пробуем найти колонку со временем
            time_columns = ['begin', 'end', 'tradedate', 'time']
            for col in time_columns:
                if col in df.columns:
                    df.rename(columns={col: 'time'}, inplace=True)
                    break
            
            # Оставляем только нужные колонки
            needed_columns = ['open', 'high', 'low', 'close', 'volume', 'time']
            available_columns = [col for col in needed_columns if col in df.columns]
            
            if len(available_columns) < 6:
                logger.error(f"Не хватает колонок для {ticker}. Есть: {df.columns.tolist()}")
                return pd.DataFrame()
            
            df = df[available_columns].copy()
            
            # Конвертируем время
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
            
            # Сортируем по времени
            df.sort_index(inplace=True)
            
            logger.info(f"Успешно загружено {len(df)} записей для {ticker}")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка загрузки истории для {ticker}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def get_multiple_history(self, tickers: List[str], days: int) -> Dict[str, pd.DataFrame]:
        """
        Получает исторические данные для нескольких инструментов.
        
        Args:
            tickers: Список тикеров
            days: Количество дней истории
            
        Returns:
            Словарь {тикер: DataFrame}
        """
        result = {}
        for ticker in tickers:
            result[ticker] = self.get_history(ticker, days)
        return result
    
    def get_last_candles(self, ticker: str, lookback: int = 100) -> pd.DataFrame:
        """
        Получает последние N свечей для инструмента.
        
        Args:
            ticker: Тикер инструмента
            lookback: Количество последних свечей
            
        Returns:
            DataFrame с последними свечами
        """
        try:
            url = f"{self.base_url}/engines/stock/markets/shares/securities/{ticker}/candles.json"
            params = {
                'interval': 24,  # daily candles
                'limit': lookback
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'candles' not in data:
                return pd.DataFrame()
            
            candles_data = data['candles']
            columns = candles_data.get('columns', [])
            candles = candles_data.get('data', [])
            
            if not candles:
                return pd.DataFrame()
            
            df = pd.DataFrame(candles, columns=columns)
            
            # Переименовываем колонки
            if 'begin' in df.columns:
                df.rename(columns={'begin': 'time'}, inplace=True)
            elif 'end' in df.columns:
                df.rename(columns={'end': 'time'}, inplace=True)
            
            # Оставляем нужные колонки
            needed = ['open', 'high', 'low', 'close', 'volume', 'time']
            available = [col for col in needed if col in df.columns]
            df = df[available].copy()
            
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка получения последних свечей для {ticker}: {e}")
            return pd.DataFrame()
    
    def get_all_last_candles(self, tickers: List[str], lookback: int = 100) -> Dict[str, pd.DataFrame]:
        """
        Получает последние свечи для всех инструментов.
        
        Args:
            tickers: Список тикеров
            lookback: Количество последних свечей
            
        Returns:
            Словарь {тикер: DataFrame}
        """
        result = {}
        for ticker in tickers:
            result[ticker] = self.get_last_candles(ticker, lookback)
        return result
    
    def get_order_book(self, ticker: str) -> Dict:
        """
        Получает стакан заявок для инструмента.
        
        Args:
            ticker: Тикер инструмента
            
        Returns:
            Словарь с данными стакана
        """
        try:
            url = f"{self.base_url}/engines/stock/markets/shares/securities/{ticker}/orderbook.json"
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            return data.get('orderbook', {})
            
        except Exception as e:
            logger.error(f"Ошибка получения стакана для {ticker}: {e}")
            return {}