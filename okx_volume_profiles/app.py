"""
    Веб-дашборд со встроенным сборщиком данных

    Функции дашборда:
    - Автоматический сбор данных через OKX WebSocket API
    - Веб-дашборд с графиками volume profile
    - Анализ доминации объемов BTC vs ETH
    - Автоматическая очистка старых данных
    - Корректное завершение при нажатии Ctrl+C
"""

# импорт модулей Python
import asyncio
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import json
import time
import os
import glob
import signal
import atexit
from datetime import datetime, timedelta
import threading
import sys
from pathlib import Path
from collections import deque

sys.path.append(str(Path(__file__).parent / "src"))

from api.okx_client import OKXWebSocketClient
from data.data_manager import DataManager


class DataBuffer:
    """
        DataBuffer - менеджер данных сделок (BTC, ETH) в реальном времени.
        Сделки хранятся в деках, длиной 1000
    """
    
    def __init__(self):
        self.btc_trades = deque(maxlen=1000)
        self.eth_trades = deque(maxlen=1000)
        
        # аттрибут для блокировки 
        self.lock = threading.Lock()
        
        self.add_count = 0
        
    def log(self, message: str):
        # логируем сообщение по времени
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {message}")
    
    def add_trade(self, inst_id: str, trade_data: dict):
        """
            add_trade() - добавление сделки в соответствующую очередь с логированием.
            
            Args:
                inst_id (str):      инентификатор инструмента (например, 'BTC-USDT')
                trade_data (dict):  данные о сделке (цена, объем, время и т.д.)
        """
        self.add_count += 1
        
        with self.lock:
            if 'BTC' in inst_id.upper():
                self.btc_trades.append(trade_data)
                
                # if self.add_count % 10 == 0:
                #     self.log(f"*** BTC сделок: {len(self.btc_trades)}")
            elif 'ETH' in inst_id.upper():
                self.eth_trades.append(trade_data)
                
                # if self.add_count % 10 == 0:
                #     self.log(f"*** ETH сделок: {len(self.eth_trades)}")
    
    def get_btc_trades(self, limit=100):
        """
            Геттер для последних limit сделок BTC
        """
        with self.lock:
            return list(self.btc_trades)[-limit:]
    
    def get_eth_trades(self, limit=100):
        """
            Геттер для последних limit сделок ETH
        """
        with self.lock:
            return list(self.eth_trades)[-limit:]

####### GLOBAL VARIABLES #######

# создаем менеджер глобально
data = DataBuffer()

# создаем глобальную ссылку на дэшборд (для корректного завершения)
dashboard_instance = None
# флаг для предовтращения повторных вызовов о завершении
cleanup_called = False

################################

def cleanup_handler(signum=None, frame=None):
    """
        Обработчик сигналов для завершение веб-приложения
    """
    global dashboard_instance, cleanup_called
    
    if cleanup_called:
        return
    cleanup_called = True
    
    if dashboard_instance:
        print("\nПолучен сигнал завершения...")
        dashboard_instance.stop_collector()
        print("Приложение корректно завершено")

    # завершаем приложение
    sys.exit(0)

def atexit_cleanup():
    """
        Обработчик для atexit - вызывается при завершении процесса.
        Дополнительная защита для корректного завершения сборщика данных
    """
    global dashboard_instance
    if dashboard_instance:
        dashboard_instance.stop_collector()

# регистрация обработчиков сигналов для корректного завершения
signal.signal(signal.SIGINT, cleanup_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, cleanup_handler)  # команда kill
atexit.register(atexit_cleanup)                 # при завершении процесса


class Dashboard:
    """
        Основной класс веб-дашборда с встроенным сборщиком данных:
        - веб-интерфейс на базе Dash/Plotly
        - сборщик данных через OKX WebSocket API
        - обработку и визуализацию данных в реальном времени
    
        Логика:
            - веб-интерфейс работает в основном потоке
            - Сборщик данных работает в отдельном потоке
            - данные передаются через глобальную переменную data
            - автообновление интерфейса каждые 3 секунды
    """
    
    def __init__(self):
        self.app = dash.Dash(__name__)
        
        # менеджер для сохранения данных в файлы
        self.data_manager = DataManager()
        
        # настройка интерфейса и обработчиков
        self.setup_layout()
        self.setup_callbacks()
        
        # состояние сборщика данных
        self.collector_running = False
        self.client = None
        self.collector_thread = None
        
        # очищаем старые данные при запуске
        self.cleanup_old_data()
        
        # запускаем сборщик при инициализации
        self.start_collector()
    
    def cleanup_old_data(self):
        try:
            data_dir = Path("data/raw")
            if not data_dir.exists():
                return
            
            deleted_count = 0
            
            trades_dir = data_dir / "trades"
            if trades_dir.exists():
                for file_path in trades_dir.glob("*.jsonl"):
                    file_path.unlink()
                    deleted_count += 1
            
            books_dir = data_dir / "books"
            if books_dir.exists():
                for file_path in books_dir.glob("*.jsonl"):
                    file_path.unlink()
                    deleted_count += 1
            
            other_dir = data_dir / "other"
            if other_dir.exists():
                for file_path in other_dir.glob("*.jsonl"):
                    file_path.unlink()
                    deleted_count += 1
            
            if deleted_count > 0:
                print(f"Очищено {deleted_count} файлов данных")
            else:
                print("Файлы данных не найдены")
                
        except Exception as e:
            print(f"Ошибка очистки данных: {e}")
        

    def setup_layout(self):
        """
            setup_layout() - настройка макета веб-интерфейса.
        """
        self.app.layout = html.Div([
            # Заголовок приложения
            html.H1("OKX Volume Profile - Dashboard", 
                   style={'textAlign': 'center', 'marginBottom': 30}),
            
            # Секция 1: Статистика BTC и ETH в одной строке
            html.Div([
                html.Div([
                    html.H3("BTC", style={'textAlign': 'center', 'color': '#f7931a', 'marginBottom': 10}),
                    html.Div(id='btc-stats', style={'fontSize': '14px', 'textAlign': 'center', 'padding': '10px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px'})
                ], className='six columns', style={'padding': '10px'}),
                
                html.Div([
                    html.H3("ETH", style={'textAlign': 'center', 'color': '#627eea', 'marginBottom': 10}),
                    html.Div(id='eth-stats', style={'fontSize': '14px', 'textAlign': 'center', 'padding': '10px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px'})
                ], className='six columns', style={'padding': '10px'}),
            ], className='row', style={'marginBottom': 20}),
            
            # Секция 2: графики Volume Profile в одной строке
            html.Div([
                html.Div([
                    html.H4("BTC Volume Profile", style={'textAlign': 'center', 'color': '#f7931a', 'marginBottom': 15}),
                    dcc.Graph(id='btc-chart')
                ], className='six columns', style={'padding': '10px'}),
                
                html.Div([
                    html.H4("ETH Volume Profile", style={'textAlign': 'center', 'color': '#627eea', 'marginBottom': 15}),
                    dcc.Graph(id='eth-chart')
                ], className='six columns', style={'padding': '10px'}),
            ], className='row', style={'marginBottom': 30}),
            
            # Секция 3: график сравнения объемов
            html.Div([
                html.Div([
                    html.H3("Объемы BTC vs ETH", style={'textAlign': 'center', 'color': '#2ecc71', 'marginBottom': 20}),
                    dcc.Graph(id='dominance-chart')
                ], className='twelve columns'),
            ], className='row', style={'marginBottom': 30}),
            
            # автообновление каждые 3 секунды
            dcc.Interval(
                id='interval-component',
                interval=3000,
                n_intervals=0
            )
        ])
    
    def setup_callbacks(self):
        """
            Настройка callback-функций для автообновления интерфейса:
                1. update_stats - обновляет статистику BTC и ETH
                2. update_btc_chart - обновляет график Volume Profile для BTC
                3. update_eth_chart - обновляет график Volume Profile для ETH
                4. update_dominance_chart - обновляет график доминации
        """
        
        @self.app.callback(
            [Output('btc-stats', 'children'),
             Output('eth-stats', 'children')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_stats(n):
            try:
                # Получаем последние 50 сделок для каждой валюты
                btc_trades = data.get_btc_trades(50)
                eth_trades = data.get_eth_trades(50)
                
                # Формируем статистику для BTC
                if btc_trades:
                    btc_price = btc_trades[-1]['price']  # Цена последней сделки
                    btc_volume = sum(t['volume'] for t in btc_trades)  # Общий объем
                    btc_text = f"Цена: ${btc_price:,.2f}   Сделок: {len(btc_trades)}   Объем: {btc_volume:.2f}"
                else:
                    btc_text = "Нет данных"
                
                # Формируем статистику для ETH
                if eth_trades:
                    eth_price = eth_trades[-1]['price']  # Цена последней сделки
                    eth_volume = sum(t['volume'] for t in eth_trades)  # Общий объем
                    eth_text = f"Цена: ${eth_price:,.2f}   Сделок: {len(eth_trades)}   Объем: {eth_volume:.2f}"
                else:
                    eth_text = "Нет данных"
                
                return btc_text, eth_text
            except Exception as e:
                return f"Ошибка: {str(e)}", f"Ошибка: {str(e)}"
        
        @self.app.callback(
            Output('btc-chart', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_btc_chart(n):
            try:
                # Получаем последние 200 сделок BTC
                trades = data.get_btc_trades(200)
                
                if not trades:
                    return go.Figure().add_annotation(
                        text="Нет данных для BTC",
                        xref="paper", yref="paper",
                        x=0.5, y=0.5, showarrow=False
                    )
                
                # Извлекаем цены и объемы из сделок
                prices = [t['price'] for t in trades]
                volumes = [t['volume'] for t in trades]
                
                # Создаем ценовые уровни (bins) для группировки
                min_price = min(prices)
                max_price = max(prices)
                price_range = max_price - min_price
                bins = 20  # Количество ценовых уровней
                bin_size = price_range / bins if price_range > 0 else 1
                
                # Группируем объемы по ценовым уровням
                volume_by_price = {}
                for price, volume in zip(prices, volumes):
                    bin_idx = int((price - min_price) / bin_size)
                    price_level = min_price + bin_idx * bin_size + bin_size / 2
                    
                    if price_level not in volume_by_price:
                        volume_by_price[price_level] = 0
                    volume_by_price[price_level] += volume
                
                # Сортируем по ценам для корректного отображения
                sorted_prices = sorted(volume_by_price.keys())
                sorted_volumes = [volume_by_price[p] for p in sorted_prices]
                
                # Создаем горизонтальную гистограмму
                fig = go.Figure(data=[
                    go.Bar(
                        y=sorted_prices,
                        x=sorted_volumes,
                        orientation='h',  # Горизонтальная ориентация
                        marker_color='rgba(247, 147, 26, 0.7)',  # Оранжевый цвет BTC
                        name='BTC Volume'
                    )
                ])
                
                # Настраиваем внешний вид графика
                fig.update_layout(
                    title=f"BTC Volume Profile ({len(trades)} сделок)",
                    xaxis_title="Volume",
                    yaxis_title="Price Level",
                    height=400
                )
                
                return fig
                
            except Exception as e:
                return go.Figure().add_annotation(
                    text=f"Ошибка BTC: {str(e)}",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
        
        @self.app.callback(
            Output('eth-chart', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_eth_chart(n):
            try:
                # Получаем последние 200 сделок ETH
                trades = data.get_eth_trades(200)
                
                if not trades:
                    return go.Figure().add_annotation(
                        text="Нет данных для ETH",
                        xref="paper", yref="paper",
                        x=0.5, y=0.5, showarrow=False
                    )
                
                # Извлекаем цены и объемы из сделок
                prices = [t['price'] for t in trades]
                volumes = [t['volume'] for t in trades]
                
                # Создаем ценовые уровни (bins) для группировки
                min_price = min(prices)
                max_price = max(prices)
                price_range = max_price - min_price
                bins = 20  # Количество ценовых уровней
                bin_size = price_range / bins if price_range > 0 else 1
                
                # Группируем объемы по ценовым уровням
                volume_by_price = {}
                for price, volume in zip(prices, volumes):
                    bin_idx = int((price - min_price) / bin_size)
                    price_level = min_price + bin_idx * bin_size + bin_size / 2
                    
                    if price_level not in volume_by_price:
                        volume_by_price[price_level] = 0
                    volume_by_price[price_level] += volume
                
                # Сортируем по ценам для корректного отображения
                sorted_prices = sorted(volume_by_price.keys())
                sorted_volumes = [volume_by_price[p] for p in sorted_prices]
                
                # Создаем горизонтальную гистограмму
                fig = go.Figure(data=[
                    go.Bar(
                        y=sorted_prices,
                        x=sorted_volumes,
                        orientation='h',  # Горизонтальная ориентация
                        marker_color='rgba(98, 126, 234, 0.7)',  # Синий цвет ETH
                        name='ETH Volume'
                    )
                ])
                
                # Настраиваем внешний вид графика
                fig.update_layout(
                    title=f"ETH Volume Profile ({len(trades)} сделок)",
                    xaxis_title="Volume",
                    yaxis_title="Price Level",
                    height=400
                )
                
                return fig
                
            except Exception as e:
                return go.Figure().add_annotation(
                    text=f"Ошибка ETH: {str(e)}",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
        
        @self.app.callback(
            Output('dominance-chart', 'figure'),
            [Input('interval-component', 'n_intervals')]
        )
        def update_dominance_chart(n):
            try:
                # Получаем последние 100 сделок для каждой валюты
                btc_trades = data.get_btc_trades(100)
                eth_trades = data.get_eth_trades(100)
                
                # Вычисляем общие объемы торгов
                btc_volume = sum(t['volume'] for t in btc_trades)
                eth_volume = sum(t['volume'] for t in eth_trades)
                total_volume = btc_volume + eth_volume
                
                # Вычисляем проценты доминации
                if total_volume == 0:
                    # Если нет данных, показываем равное распределение
                    btc_pct = 50
                    eth_pct = 50
                else:
                    btc_pct = (btc_volume / total_volume) * 100
                    eth_pct = (eth_volume / total_volume) * 100
                
                # Создаем круговую диаграмму
                fig = go.Figure(data=[
                    go.Pie(
                        labels=['BTC', 'ETH'],
                        values=[btc_pct, eth_pct],
                        hole=0.3,  # Дырка в центре для современного вида
                        marker_colors=['#f7931a', '#627eea']  # Цвета BTC и ETH
                    )
                ])
                
                # Настраиваем внешний вид графика
                fig.update_layout(
                    title=f"Доминация BTC vs ETH<br>BTC: {btc_pct:.1f}% | ETH: {eth_pct:.1f}%",
                    height=400
                )
                
                return fig
                
            except Exception as e:
                return go.Figure().add_annotation(
                    text=f"Ошибка доминации: {str(e)}",
                    xref="paper", yref="paper",
                    x=0.5, y=0.5, showarrow=False
                )
        
    
    def handle_data(self, msg: dict):
        """
            Обрабатывает входящие данные от WebSocket API OKX.
 
            Args:
                msg (dict): Сообщение от OKX WebSocket API
        """
        try:
            # Сохраняем сырые данные в файлы для архивирования
            self.data_manager.save_raw_data(msg)
            
            # Обрабатываем только сообщения с данными о сделках
            if 'data' in msg and msg['data']:
                arg = msg.get('arg', {})
                inst_id = arg.get('instId', '')
                channel = arg.get('channel', '')
                
                # Проверяем, что это канал сделок (trades)
                if channel == 'trades':
                    for trade in msg['data']:
                        # Проверяем наличие обязательных полей в сделке
                        required_fields = ['px', 'sz', 'ts', 'side']  # цена, размер, время, сторона
                        missing_fields = [field for field in required_fields if field not in trade]
                        
                        # Пропускаем сделки с неполными данными
                        if missing_fields:
                            continue
                        
                        try:
                            # Формируем структурированные данные о сделке
                            trade_data = {
                                'timestamp': int(trade['ts']),      # Время сделки (Unix timestamp)
                                'price': float(trade['px']),        # Цена сделки
                                'volume': float(trade['sz']),       # Объем сделки
                                'side': trade['side'],              # Сторона (buy/sell)
                                'trade_id': trade.get('tradeId', ''), # ID сделки
                                'inst_id': trade.get('instId', inst_id) # Инструмент
                            }
                            
                            # Добавляем сделку в глобальный буфер данных
                            data.add_trade(inst_id, trade_data)
                            
                        except Exception as e:
                            # Игнорируем ошибки обработки отдельных сделок
                            pass
                            
        except Exception as e:
            # Игнорируем ошибки обработки сообщений
            pass
    
    def start_collector(self):
        """
            Запускает сборщик данных в отдельном потоке для подключения к OKX websocket API и сбора данных
        """
        if self.collector_running:
            return  # уже запущен
            
        self.collector_running = True
        
        def run_collector():
            """
                Внутренняя функция для запуска асинхронного сборщика.
            """
            async def collect_data():
                try:
                    # создаем клиент WebSocket для OKX API
                    self.client = OKXWebSocketClient(
                        data_handler=self.handle_data
                    )
                    
                    # каналы для подписки
                    channels = [
                        {"channel": "trades", "instId": "BTC-USDT"},
                        {"channel": "trades", "instId": "ETH-USDT"},
                    ]
                    
                    print("Подключение к OKX WebSocket API...")
                    print("Каналы:", [f"{ch['channel']}:{ch['instId']}" for ch in channels])
                    
                    # Запускаем клиент и начинаем сбор данных
                    await self.client.connect_and_listen(channels)
                    
                except Exception as e:
                    print(f"Ошибка сборщика: {e}")
                finally:
                    if self.client:
                        try:
                            await self.client.close()
                            print("🔌 WebSocket соединение закрыто")
                        except:
                            pass
            
            # запускаем асинхронную функцию в новом event loop
            asyncio.run(collect_data())
        
        # отдельный поток
        self.collector_thread = threading.Thread(target=run_collector, daemon=True)
        self.collector_thread.start()
        
        print("Сборщик данных запущен в фоновом режиме")
    
    def stop_collector(self):
        if not self.collector_running:
            return  # сборщик остановлен
            
        self.collector_running = False
        print("Остановка сборщика данных...")
        
        # Ждем завершения потока сборщика
        if self.collector_thread and self.collector_thread.is_alive():
            self.collector_thread.join(timeout=5)
            if self.collector_thread.is_alive():
                print("Поток сборщика не завершился в течение 5 секунд")
            else:
                print("Сборщик данных остановлен")
    
    def run(self, host='127.0.0.1', port=8050, debug=False):
        print(f"Запуск рабочего дашборда на http://{host}:{port}")
        print("Откройте браузер и перейдите по указанному адресу")
        print("Для остановки нажмите Ctrl+C")
        
        print("Ожидание накопления данных (5 секунд)...")
        time.sleep(5)
        
        self.app.run(host=host, port=port, debug=debug)


def main():
    global dashboard_instance
    dashboard_instance = Dashboard()
    dashboard_instance.run(debug=True)


if __name__ == "__main__":
    main()
