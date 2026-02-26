# screener/reporters.py
"""
Модуль для формирования отчетов по результатам анализа акций.
Работает с обновленным StockScreener и StrategyMatcher.
"""

import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict
import logging
import os

logger = logging.getLogger(__name__)

class BaseReporter:
    """Базовый класс для всех репортеров."""
    
    def __init__(self, data: pd.DataFrame = None):
        """
        Инициализация базового репортера.
        
        Args:
            data: DataFrame с результатами анализа
        """
        self.data = data if data is not None else pd.DataFrame()
        self.timestamp = datetime.now()
    
    def set_data(self, data: pd.DataFrame):
        """Устанавливает данные для отчета."""
        self.data = data
        self.timestamp = datetime.now()


class ConsoleReporter(BaseReporter):
    """
    Репортер для вывода результатов в консоль.
    Формирует красивое табличное представление.
    """
    
    def print_header(self, title: str):
        """Печатает заголовок."""
        print("\n" + "="*90)
        print(f"📊 {title:^86}")
        print("="*90)
    
    def print_section(self, title: str):
        """Печатает заголовок секции."""
        print("\n" + "─"*90)
        print(f"📌 {title}")
        print("─"*90)
    
    def print_top_stocks(self, title: str = "ТОП АКЦИЙ", limit: int = 10):
        """
        Печатает таблицу с топ акциями.
        
        Args:
            title: Заголовок таблицы
            limit: Количество строк для отображения
        """
        if self.data.empty:
            print("❌ Нет данных для отображения")
            return
        
        df = self.data.head(limit)
        
        self.print_header(title)
        
        # Определяем доступные колонки
        columns = []
        headers = []
        
        if 'ticker' in df.columns:
            columns.append('ticker')
            headers.append('Тикер')
        
        if 'name' in df.columns:
            columns.append('name')
            headers.append('Компания')
        
        if 'sector' in df.columns:
            columns.append('sector')
            headers.append('Сектор')
        
        if 'price' in df.columns:
            columns.append('price')
            headers.append('Цена')
        
        if 'rsi' in df.columns:
            columns.append('rsi')
            headers.append('RSI')
        
        if 'trend' in df.columns:
            columns.append('trend')
            headers.append('Тренд')
        
        if 'score' in df.columns:
            columns.append('score')
            headers.append('Оценка')
        
        if 'recommendation' in df.columns:
            columns.append('recommendation')
            headers.append('Рекомендация')
        
        # Формируем строку формата
        col_widths = [15, 30, 15, 10, 8, 8, 8, 25]
        format_str = ""
        for i, width in enumerate(col_widths[:len(headers)]):
            format_str += f"{{{i}:<{width}}}"
        
        # Печатаем заголовки
        print("\n" + format_str.format(*headers))
        print("-"*90)
        
        # Печатаем данные
        for _, row in df.iterrows():
            values = []
            for col in columns:
                if col == 'trend':
                    trend_emoji = "📈" if row[col] == 'up' else "📉" if row[col] == 'down' else "➡️"
                    values.append(f"{trend_emoji} {row[col]}")
                elif col == 'price':
                    values.append(f"{row[col]:.2f}")
                elif col == 'rsi':
                    values.append(f"{row[col]:.1f}")
                elif col == 'score':
                    values.append(f"{row[col]:.1f}")
                else:
                    values.append(str(row[col])[:col_widths[columns.index(col)]-2])
            
            print(format_str.format(*values))
        
        print("\n" + "="*90)
    
    def print_strategy_recommendations(self, strategy_results: Dict[str, pd.DataFrame]):
        """
        Печатает рекомендации по стратегиям.
        
        Args:
            strategy_results: Словарь с результатами по стратегиям
        """
        self.print_header("РЕКОМЕНДАЦИИ ПО СТРАТЕГИЯМ")
        
        strategy_titles = {
            'rsi': 'RSI Mean Reversion',
            'sma': 'SMA Crossover',
            'momentum': 'Momentum',
            'value': 'Value'
        }
        
        for strategy, df in strategy_results.items():
            if df is None or df.empty:
                continue
            
            title = strategy_titles.get(strategy, strategy.upper())
            self.print_section(title)
            
            score_col = f"{strategy}_score"
            signal_col = f"{strategy}_signal"
            
            for idx, row in df.head(5).iterrows():
                score = row.get(score_col, 0)
                signal = row.get(signal_col, '')
                ticker = row.get('ticker', 'N/A')
                name = row.get('name', '')[:25]
                
                stars = "⭐" * min(3, int(score/20))
                print(f"  {idx+1}. {stars} {ticker:<6} - {name:<25} | {signal}")
    
    def print_pairs(self, pairs: List[Dict]):
        """
        Печатает найденные пары для парного трейдинга.
        
        Args:
            pairs: Список словарей с парами
        """
        if not pairs:
            print("❌ Потенциальных пар не найдено")
            return
        
        self.print_section("ПАРЫ ДЛЯ ТРЕЙДИНГА")
        
        for idx, pair in enumerate(pairs[:5], 1):
            strength_emoji = "💪" if "СИЛЬНАЯ" in pair['strength'] else "👌" if "СРЕДНЯЯ" in pair['strength'] else "🤏"
            
            print(f"\n  {idx}. {strength_emoji} {pair['sector']}")
            print(f"     {pair['asset1']} / {pair['asset2']}")
            print(f"     Z-score: {pair['current_z']:.2f} | {pair['strength']}")
    
    def print_summary(self, summary_df: pd.DataFrame):
        """
        Печатает сводный отчет.
        
        Args:
            summary_df: DataFrame со сводными данными
        """
        if summary_df.empty:
            return
        
        self.print_header("СВОДНЫЙ ОТЧЕТ")
        
        # Статистика по секторам
        if 'sector' in summary_df.columns:
            self.print_section("РАСПРЕДЕЛЕНИЕ ПО СЕКТОРАМ")
            sector_stats = summary_df['sector'].value_counts()
            for sector, count in sector_stats.items():
                print(f"  {sector}: {count} акций")
        
        # Статистика по рекомендациям
        if 'recommendation' in summary_df.columns:
            self.print_section("СТАТИСТИКА РЕКОМЕНДАЦИЙ")
            rec_stats = summary_df['recommendation'].value_counts()
            for rec, count in rec_stats.items():
                print(f"  {rec}: {count}")
        
        # Лучшие и худшие
        if 'score' in summary_df.columns:
            self.print_section("ЭКСТРЕМУМЫ")
            best = summary_df.nlargest(1, 'score').iloc[0]
            worst = summary_df.nsmallest(1, 'score').iloc[0]
            
            print(f"  🏆 Лучшая: {best['ticker']} - {best.get('name', '')[:30]} (оценка: {best['score']:.1f})")
            print(f"  📉 Худшая: {worst['ticker']} - {worst.get('name', '')[:30]} (оценка: {worst['score']:.1f})")


class HTMLReporter(BaseReporter):
    """
    Репортер для создания HTML отчетов.
    Генерирует красивые веб-страницы с результатами анализа.
    """
    
    def __init__(self, data: pd.DataFrame = None):
        super().__init__(data)
        self.css_styles = """
        <style>
            body { 
                font-family: 'Segoe UI', Arial, sans-serif; 
                margin: 30px; 
                background-color: #f5f5f5;
                color: #333;
            }
            .container {
                max-width: 1400px;
                margin: 0 auto;
                background-color: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 { 
                color: #2c3e50; 
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }
            h2 { 
                color: #34495e; 
                margin-top: 30px;
                border-left: 5px solid #3498db;
                padding-left: 15px;
            }
            table { 
                border-collapse: collapse; 
                width: 100%; 
                margin: 20px 0;
                background-color: white;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }
            th { 
                background-color: #3498db; 
                color: white; 
                padding: 12px; 
                text-align: left; 
                font-weight: 600;
            }
            td { 
                padding: 10px; 
                border-bottom: 1px solid #ecf0f1; 
            }
            tr:hover { 
                background-color: #f8f9fa; 
            }
            .buy-strong { 
                background-color: #d4edda; 
                color: #155724;
                font-weight: bold;
            }
            .buy { 
                background-color: #d4edda; 
                color: #155724;
            }
            .watch { 
                background-color: #fff3cd; 
                color: #856404;
            }
            .avoid { 
                background-color: #f8d7da; 
                color: #721c24;
            }
            .neutral { 
                background-color: #e2e3e5; 
                color: #383d41;
            }
            .footer { 
                margin-top: 30px; 
                color: #7f8c8d; 
                text-align: center;
                font-size: 0.9em;
            }
            .badge {
                display: inline-block;
                padding: 3px 7px;
                border-radius: 3px;
                font-size: 0.8em;
                font-weight: bold;
            }
            .badge-up { background-color: #d4edda; color: #155724; }
            .badge-down { background-color: #f8d7da; color: #721c24; }
            .badge-neutral { background-color: #e2e3e5; color: #383d41; }
            .stat-box {
                display: inline-block;
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 5px;
                padding: 15px;
                margin: 10px;
                min-width: 200px;
            }
            .stat-number {
                font-size: 24px;
                font-weight: bold;
                color: #3498db;
            }
        </style>
        """
    
    def generate_html(self, title: str = "Анализ акций MOEX") -> str:
        """
        Генерирует HTML код отчета.
        
        Args:
            title: Заголовок страницы
            
        Returns:
            HTML строка
        """
        if self.data.empty:
            return "<html><body><h1>Нет данных для отображения</h1></body></html>"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
            <meta charset="utf-8">
            {self.css_styles}
        </head>
        <body>
            <div class="container">
                <h1>📊 {title}</h1>
                <p>Отчет сгенерирован: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Проанализировано акций: <strong>{len(self.data)}</strong></p>
        """
        
        # Статистика
        html += self._generate_stats()
        
        # Основная таблица
        html += self._generate_main_table()
        
        # Распределение по секторам
        html += self._generate_sector_distribution()
        
        # Худшие акции
        html += self._generate_worst_stocks()
        
        html += """
                <div class="footer">
                    <p>Сгенерировано с использованием moexalgo</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _generate_stats(self) -> str:
        """Генерирует блок со статистикой."""
        html = "<h2>📈 ОБЩАЯ СТАТИСТИКА</h2><div style='display: flex; flex-wrap: wrap;'>"
        
        # Средняя оценка
        avg_score = self.data['score'].mean()
        html += f"""
            <div class="stat-box">
                <div>Средняя оценка</div>
                <div class="stat-number">{avg_score:.1f}</div>
            </div>
        """
        
        # Медианная оценка
        median_score = self.data['score'].median()
        html += f"""
            <div class="stat-box">
                <div>Медианная оценка</div>
                <div class="stat-number">{median_score:.1f}</div>
            </div>
        """
        
        # Распределение трендов
        if 'trend' in self.data.columns:
            up_count = len(self.data[self.data['trend'] == 'up'])
            down_count = len(self.data[self.data['trend'] == 'down'])
            neutral_count = len(self.data[self.data['trend'] == 'neutral'])
            
            html += f"""
                <div class="stat-box">
                    <div>Тренды</div>
                    <div>
                        <span class="badge badge-up">📈 {up_count}</span>
                        <span class="badge badge-neutral">➡️ {neutral_count}</span>
                        <span class="badge badge-down">📉 {down_count}</span>
                    </div>
                </div>
            """
        
        # Рекомендации
        if 'recommendation' in self.data.columns:
            buy_strong = len(self.data[self.data['recommendation'].str.contains("СИЛЬНАЯ", na=False)])
            buy = len(self.data[self.data['recommendation'].str.contains("ПОКУПКА", na=False) & 
                                ~self.data['recommendation'].str.contains("СИЛЬНАЯ", na=False)])
            avoid = len(self.data[self.data['recommendation'].str.contains("ИЗБЕГАТЬ", na=False)])
            
            html += f"""
                <div class="stat-box">
                    <div>Рекомендации</div>
                    <div>
                        <span class="badge badge-up">🚀 {buy_strong}</span>
                        <span class="badge badge-up">✅ {buy}</span>
                        <span class="badge badge-down">❌ {avoid}</span>
                    </div>
                </div>
            """
        
        html += "</div>"
        return html
    
    def _generate_main_table(self) -> str:
        """Генерирует основную таблицу с акциями."""
        html = "<h2>📋 ПОЛНЫЙ СПИСОК АКЦИЙ</h2>"
        html += "<table><tr>"
        
        # Заголовки
        headers = {
            'ticker': 'Тикер',
            'name': 'Компания',
            'sector': 'Сектор',
            'price': 'Цена',
            'rsi': 'RSI',
            'trend': 'Тренд',
            'score': 'Оценка',
            'recommendation': 'Рекомендация'
        }
        
        for col, header in headers.items():
            if col in self.data.columns:
                html += f"<th>{header}</th>"
        
        html += "</tr>"
        
        # Данные
        for _, row in self.data.iterrows():
            row_class = ""
            if 'recommendation' in row and pd.notna(row['recommendation']):
                if "СИЛЬНАЯ ПОКУПКА" in str(row['recommendation']):
                    row_class = "buy-strong"
                elif "ПОКУПКА" in str(row['recommendation']):
                    row_class = "buy"
                elif "ИЗБЕГАТЬ" in str(row['recommendation']):
                    row_class = "avoid"
                elif "НЕЙТРАЛЬНО" in str(row['recommendation']):
                    row_class = "neutral"
                else:
                    row_class = "watch"
            
            html += f"<tr class='{row_class}'>"
            
            for col in headers.keys():
                if col in self.data.columns:
                    value = row[col]
                    if col == 'price' and pd.notna(value):
                        html += f"<td>{value:.2f}</td>"
                    elif col == 'rsi' and pd.notna(value):
                        html += f"<td>{value:.1f}</td>"
                    elif col == 'score' and pd.notna(value):
                        html += f"<td>{value:.1f}</td>"
                    elif col == 'trend' and pd.notna(value):
                        emoji = "📈" if value == 'up' else "📉" if value == 'down' else "➡️"
                        html += f"<td>{emoji} {value}</td>"
                    else:
                        html += f"<td>{value if pd.notna(value) else ''}</td>"
            
            html += "</tr>"
        
        html += "</table>"
        return html
    
    def _generate_sector_distribution(self) -> str:
        """Генерирует таблицу распределения по секторам."""
        if 'sector' not in self.data.columns:
            return ""
        
        html = "<h2>🏢 РАСПРЕДЕЛЕНИЕ ПО СЕКТОРАМ</h2>"
        html += "<table><tr><th>Сектор</th><th>Количество</th><th>Средняя оценка</th><th>Лучший</th></tr>"
        
        sector_stats = self.data.groupby('sector').agg({
            'ticker': 'count',
            'score': 'mean'
        }).round(1)
        
        for sector in sector_stats.index:
            sector_data = self.data[self.data['sector'] == sector]
            best = sector_data.nlargest(1, 'score').iloc[0]
            
            html += f"""
                <tr>
                    <td><b>{sector}</b></td>
                    <td>{int(sector_stats.loc[sector, 'ticker'])}</td>
                    <td>{sector_stats.loc[sector, 'score']:.1f}</td>
                    <td>{best['ticker']} ({best['score']:.1f})</td>
                </tr>
            """
        
        html += "</table>"
        return html
    
    def _generate_worst_stocks(self) -> str:
        """Генерирует таблицу с худшими акциями."""
        if 'score' not in self.data.columns:
            return ""
        
        worst = self.data.nsmallest(5, 'score')
        
        html = "<h2>⚠️ ХУДШИЕ АКЦИИ (ИЗБЕГАТЬ)</h2>"
        html += "<table><tr><th>Тикер</th><th>Компания</th><th>Оценка</th><th>RSI</th><th>Рекомендация</th></tr>"
        
        for _, row in worst.iterrows():
            html += f"""
                <tr class='avoid'>
                    <td><b>{row['ticker']}</b></td>
                    <td>{row.get('name', '')}</td>
                    <td>{row['score']:.1f}</td>
                    <td>{row.get('rsi', 0):.1f}</td>
                    <td>{row.get('recommendation', '')}</td>
                </tr>
            """
        
        html += "</table>"
        return html
    
    def save(self, filename: str = "stock_report.html"):
        """
        Сохраняет HTML отчет в файл.
        
        Args:
            filename: Имя файла для сохранения
        """
        html_content = self.generate_html()
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"HTML отчет сохранен в {os.path.abspath(filename)}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении HTML отчета: {e}")
            return False


# Для тестирования
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'ticker': ['SBER', 'GAZP', 'LKOH', 'YDEX', 'PLZL', 'AFLT'],
        'name': ['Сбербанк', 'Газпром', 'Лукойл', 'Яндекс', 'Полюс', 'Аэрофлот'],
        'sector': ['Finance', 'Energy', 'Energy', 'IT', 'Metals', 'Transport'],
        'price': [250.5, 180.3, 3500.0, 2800.0, 12500.0, 45.6],
        'rsi': [35, 68, 45, 72, 28, 55],
        'trend': ['up', 'down', 'neutral', 'up', 'up', 'down'],
        'score': [75, 45, 60, 55, 85, 40],
        'recommendation': ['СИЛЬНАЯ ПОКУПКА', 'ИЗБЕГАТЬ', 'НАБЛЮДЕНИЕ', 
                          'НАБЛЮДЕНИЕ', 'СИЛЬНАЯ ПОКУПКА', 'ИЗБЕГАТЬ']
    })
    
    print("Тестирование ConsoleReporter...")
    console = ConsoleReporter(test_data)
    console.print_top_stocks("ТЕСТОВЫЙ ОТЧЕТ", limit=6)
    
    print("\nТестирование HTMLReporter...")
    html = HTMLReporter(test_data)
    html.save("test_report.html")
    print("HTML отчет сохранен в test_report.html")
    
    print("\n✅ Тестирование reporters.py завершено")