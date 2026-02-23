"""
Менеджер агрессивного кэширования
Исправлен баг общей погоды: теперь погода кэшируется отдельно для каждой пары координат.
"""
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from config import CACHE_PATH, CACHE_TTL_SECONDS, DEFAULT_LAT, DEFAULT_LON, ENABLE_BACKGROUND_REFRESH
from api_client import APIClient

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, cache_path: Path, api_client: APIClient):
        self.cache_path = cache_path
        self.api_client = api_client
        self._refresh_lock = asyncio.Lock()
        self._bg_task: Optional[asyncio.Task] = None

    def _is_cache_valid(self, cache_item: Dict) -> bool:
        if not cache_item or "fetched_at" not in cache_item:
            return False
        try:
            fetched_str = cache_item["fetched_at"].replace("Z", "+00:00")
            fetched = datetime.fromisoformat(fetched_str)
            if fetched.tzinfo is None:
                fetched = fetched.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - fetched).total_seconds() < CACHE_TTL_SECONDS
        except (ValueError, KeyError, TypeError):
            return False

    def _load_cache_sync(self) -> Dict:
        """Возвращает весь кэш (содержит ключи 'global' и 'weather_Lat_Lon')"""
        if not self.cache_path.exists():
            return {"global": {}}
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {"global": {}}
        except Exception as e:
            logger.warning(f"Не удалось загрузить кэш: {e}")
            return {"global": {}}

    async def _load_cache(self) -> Dict:
        return await asyncio.to_thread(self._load_cache_sync)

    def _save_cache_sync(self, full_cache_data: Dict):
        try:
            temp_path = self.cache_path.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(full_cache_data, f, ensure_ascii=False, indent=2)
            temp_path.replace(self.cache_path)
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {e}")

    async def _save_cache(self, full_cache_data: Dict):
        await asyncio.to_thread(self._save_cache_sync, full_cache_data)

    def _get_coord_key(self, lat: float, lon: float) -> str:
        """Генерирует ключ для погоды с точностью до 2 знаков (~1 км)"""
        return f"weather_{round(lat, 2)}_{round(lon, 2)}"

    async def get_data(self, lat: float = DEFAULT_LAT, lon: float = DEFAULT_LON) -> Dict[str, Any]:
        """
        Собирает данные на лету: 
        Глобальные (крипта, новости) + Локальные (погода для lat/lon)
        """
        full_cache = await self._load_cache()
        global_data = full_cache.get("global", {})
        coord_key = self._get_coord_key(lat, lon)
        weather_data = full_cache.get(coord_key, {})

        global_valid = self._is_cache_valid(global_data)
        weather_valid = self._is_cache_valid(weather_data)

        # Если всё валидно — отдаём сразу
        if global_valid and weather_valid:
            result = global_data.copy()
            result["weather"] = weather_data.get("data")
            return result

        # Если что-то устарело и включен фон — запускаем обновление, отдаем старое
        if ENABLE_BACKGROUND_REFRESH and (global_data or weather_data):
            if self._bg_task is None or self._bg_task.done():
                self._bg_task = asyncio.create_task(
                    self._refresh_cache(lat, lon, not global_valid, not weather_valid)
                )
            # Собираем ответ из того, что есть
            result = global_data.copy()
            result["weather"] = weather_data.get("data")
            return result

        # Если кэш совсем пустой — блокируемся и ждём
        async with self._refresh_lock:
            full_cache = await self._load_cache()
            global_data = full_cache.get("global", {})
            weather_data = full_cache.get(coord_key, {})
            
            if self._is_cache_valid(global_data) and self._is_cache_valid(weather_data):
                result = global_data.copy()
                result["weather"] = weather_data.get("data")
                return result
                
            return await self._refresh_cache(lat, lon, not self._is_cache_valid(global_data), not self._is_cache_valid(weather_data))

    async def _refresh_cache(self, lat: float, lon: float, update_global: bool = True, update_weather: bool = True) -> Dict[str, Any]:
        full_cache = await self._load_cache()
        now_iso = datetime.now(timezone.utc).isoformat()
        coord_key = self._get_coord_key(lat, lon)
        
        # Обновляем погоду если нужно
        if update_weather:
            try:
                weather = await self.api_client.fetch_weather(lat, lon)
                full_cache[coord_key] = {"data": weather, "fetched_at": now_iso}
            except Exception as e:
                logger.error(f"Ошибка фонового обновления погоды: {e}")

        # Обновляем глобальные данные если нужно
        if update_global:
            try:
                fresh_global = await self.api_client.fetch_all_data()
                fresh_global["fetched_at"] = now_iso
                full_cache["global"] = fresh_global
            except Exception as e:
                logger.error(f"Ошибка фонового обновления глобальных данных: {e}")

        await self._save_cache(full_cache)

        # Собираем итоговый ответ для юзера, который вызвал обновление
        result = full_cache.get("global", {}).copy()
        result["weather"] = full_cache.get(coord_key, {}).get("data")
        return result

    async def force_refresh(self, lat: float = DEFAULT_LAT, lon: float = DEFAULT_LON):
        """
        Принудительное обновление. Вызывается по крону.
        Здесь мы обновляем глобальные данные и погоду по умолчанию (например, Москву).
        """
        async with self._refresh_lock:
            return await self._refresh_cache(lat, lon, update_global=True, update_weather=True)
