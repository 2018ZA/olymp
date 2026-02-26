#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Точка входа в торгового робота.
Запускает основной торговый движок.
"""

import sys
import traceback
from core.trading_engine import TradingEngine
from utils.logger import logger, setup_logging

def main():
    """Главная функция"""
    try:
        # Настраиваем логирование
        setup_logging()
        
        logger.info("=" * 60)
        logger.info("Запуск торгового робота")
        logger.info("=" * 60)
        
        # Создаем и запускаем движок
        engine = TradingEngine()
        engine.start()
        
    except KeyboardInterrupt:
        logger.info("Робот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        logger.info("=" * 60)
        logger.info("Работа робота завершена")
        logger.info("=" * 60)

if __name__ == "__main__":
    main()