"""
Менеджер данных для сохранения и обработки WebSocket сырых данных.
"""

import json
import os
from datetime import datetime
from pathlib import Path


class DataManager:
    """
    Менеджер для сохранения и обработки данных от WebSocket API.
    
    DataManager структирует полученные данные с API в файлы, сохраняемые по filepath=base_raw_path
    """

    def __init__(self, base_raw_path: str = "data/raw"):
        """
        Args:
            base_raw_path (str): базовый путь для сохранения сырых данных
        """
        self.base_raw_path = Path(base_raw_path)
        self._ensure_directories()

    def _ensure_directories(self):
        """
            _ensure_directories() оздает необходимые директории для хранения данных. Например:
                - trades - для данных о сделках
                - candles - для данных свечей
        """
        channels = ["trades", "candles", "other"]
        for channel in channels:
            (self.base_raw_path / channel).mkdir(parents=True, exist_ok=True)

    def save_raw_data(self, msg: dict):
        """
        save_raw_data(msg) сохраняет сырые данные в соответствующую папку
        
        Формат файлов: {instId}_{channel}_{YYYY-MM-DD}.jsonl
        
        Args:
            msg (dict): Сообщение от WebSocket API OKX
        """

        arg = msg.get('arg', {})
        channel = arg.get('channel', 'unknown')  
        inst_id = arg.get('instId', 'unknown')   

        if 'trade' in channel:
            folder = self.base_raw_path / "trades"
        else:
            folder = self.base_raw_path / "other"

        folder.mkdir(exist_ok=True)

        # cоздаем имя файла на основе текущей даты
        timestamp = datetime.now().strftime("%Y-%m-%d")
        filename = f"{inst_id}_{channel}_{timestamp}.jsonl"
        filepath = folder / filename

        # сериализация в jsonl
        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(json.dumps(msg, ensure_ascii=False) + '\n')