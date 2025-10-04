"""
Клиент для подключения к публичному WebSocket API OKX.
"""

import asyncio
import datetime
import json
import websockets
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable


class OKXWebSocketClient:
    """
        Клиент для подключения к публичному WebSocket API OKX.
        
        OKXWebSocketClient обеспечивает асинхронное подключение к WebSocket API OKX,
        подписку на каналы данных и обработку входящих сообщений в реальном времени.
    """

    def __init__(self, data_handler: Callable[[Dict[str, Any]], None]):
        """
            data_handler: функция-обработчик для обработки полученных данных (в виде сообщений)
        """
        # public websocket url
        self.url = 'wss://ws.okx.com:8443/ws/v5/public'
        
        self.data_handler = data_handler
        
        # объект websocket соединения
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None


    async def subscribe(self, channels: List[Dict[str, str]]) -> None:
        """
            subcribe() отправляет запрос на подписку на указанные каналы.
            
            Args:
                channels: список каналов для подписки в формате:
                        [{"channel": "trades", "instId": "BTC-USDT"}, ...]
            
            Raises:
                RuntimeError: если WebSocket соединение не установлено
        """
        if self.websocket is None:
            raise RuntimeError("WebSocket is not connected.")
        
        # Формируем аргументы для подписки в правильном формате OKX
        args = []
        for ch in channels:
            # Каждый канал должен содержать channel и instId
            args.append({
                "channel": ch['channel'], # тип канала
                "instId": ch['instId']    # id инструмента (BTC-USDT, ETH-USDT, etc.)
            })
        
        # Создаем сообщение подписки в формате OKX API
        subscription_msg = {
            "op": "subscribe",  # операция подписки
            "args": args        # список каналов с инструментом для подписки
        }
        
        # отправляем сообщение подписки
        await self.websocket.send(json.dumps(subscription_msg))
        
        # логируем информацию о подписанных каналах
        channel_names = [f"{ch['channel']}:{ch['instId']}" for ch in channels]
        print(f"Subscribed to channels: {channel_names}")

    async def connect_and_listen(self, channels: List[Dict[str, str]]) -> None:
        """
            connect_and_listen() - основной цикл подключения и прослушивания websocket.
            
            Процесс работы:
                1. Устанавливает WebSocket соединение с ping/pong
                2. Подписывается на указанные каналы
                3. Входит в цикл прослушивания сообщений
                4. Обрабатывает каждое входящее сообщение
                5. Корректно закрывает соединение при завершении

            Args:
                channels: список каналов для подписки
        """
        async with websockets.connect(
            self.url, 
            ping_interval=20,  
            ping_timeout=60    # таймаут ожидания pong
        ) as ws:
            self.websocket = ws # ссылка на websocket соединение
            print(f'Connected at {datetime.datetime.now().isoformat()}')

            await self.subscribe(channels)

            # основной цикл прослушивания сообщений
            async for raw_msg in ws:
                try:
                    # json парсинг сообщения
                    msg = json.loads(raw_msg)
                    # обрабатываем сообщение
                    await self._handle_message(msg)
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                except Exception as e:
                    print(f"Error handling message: {e}")

            # очищаем ссылку на websocket при завершении
            self.websocket = None
        print(f'Disconnected at {datetime.datetime.now().isoformat()}')

    async def _handle_message(self, msg: Dict[str, Any]) -> None:
        """
            handle_message() обрабатывает входящее сообщение от WebSocket.
            
            Args:
                msg: Словарь с данными сообщения от WebSocket API
        """
        event = msg.get('event') # поле вида: subscribe/unsubscribe/error
        data = msg.get('data')

        if event:
            # логируем ошибки
            if event == 'error':
                print(f"************* ERROR: {msg} *************")
            else:
            # логируем основные события (subscribe, unsubscribe и т.д.)
                print(f"************* Event: {event} | Arg: {msg.get('arg')} *************")
            return

        if data and len(data) > 0:
            # передаем данные обработчику
            self.data_handler(msg)
    
    async def close(self):
        """
            close() закрывает websocket соединение
        """
        if self.websocket:
            await self.websocket.close()
            self.websocket = None