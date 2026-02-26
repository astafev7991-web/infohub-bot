"""
Клиент для внешних API (погода, крипто, валюты)
RSS-новости удалены — новости теперь через NewsDigest (newsapi.org)
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable
from functools import wraps
from dataclasses import dataclass

import aiohttp

from config import (
    OPEN_METEO_BASE, COINGECKO_URL, COINGECKO_PARAMS, EXCHANGE_RATE_URL
)

logger = logging.getLogger(__name__)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)

MAX_RETRIES = 3
RETRY_DELAY_BASE = 1.0
RETRY_DELAY_MAX = 10.0

WEATHER_HOURLY_LIMIT = 10


@dataclass
class WeatherMetrics:
    hourly_calls: int = 0
    last_hour_reset: float = 0

    def can_make_request(self) -> bool:
        current_hour_start = int(time.time() // 3600) * 3600
        if self.last_hour_reset != current_hour_start:
            self.hourly_calls = 0
            self.last_hour_reset = current_hour_start
            logger.info("Weather API: Hourly counter reset")
        if self.hourly_calls >= WEATHER_HOURLY_LIMIT:
            logger.warning(f"Weather API: Hourly limit reached ({self.hourly_calls}/{WEATHER_HOURLY_LIMIT})")
            return False
        return True

    def increment(self):
        self.hourly_calls += 1

    def get_remaining(self) -> int:
        self.can_make_request()
        return WEATHER_HOURLY_LIMIT - self.hourly_calls


_weather_metrics = WeatherMetrics()


# === WMO WEATHER CODES ===
WMO_WEATHER_CODES = {
    0:  {"condition": "Ясно",               "emoji": "☀️",  "precipitation": None},
    1:  {"condition": "Малооблачно",         "emoji": "🌤️", "precipitation": None},
    2:  {"condition": "Переменная облачность","emoji": "⛅",  "precipitation": None},
    3:  {"condition": "Облачно",             "emoji": "☁️",  "precipitation": None},
    45: {"condition": "Туман",              "emoji": "🌫️", "precipitation": "туман"},
    48: {"condition": "Изморозь",           "emoji": "🌫️", "precipitation": "изморозь"},
    51: {"condition": "Морось",             "emoji": "🌦️", "precipitation": "слабая морось"},
    53: {"condition": "Морось",             "emoji": "🌧️", "precipitation": "морось"},
    55: {"condition": "Морось",             "emoji": "🌧️", "precipitation": "сильная морось"},
    61: {"condition": "Дождь",              "emoji": "🌧️", "precipitation": "слабый дождь"},
    63: {"condition": "Дождь",              "emoji": "🌧️", "precipitation": "дождь"},
    65: {"condition": "Дождь",              "emoji": "🌧️", "precipitation": "сильный дождь"},
    66: {"condition": "Ледяной дождь",      "emoji": "🌨️", "precipitation": "ледяной дождь"},
    67: {"condition": "Ледяной дождь",      "emoji": "🌨️", "precipitation": "сильный ледяной дождь"},
    71: {"condition": "Снег",               "emoji": "🌨️", "precipitation": "слабый снег"},
    73: {"condition": "Снег",               "emoji": "❄️",  "precipitation": "снег"},
    75: {"condition": "Снег",               "emoji": "❄️",  "precipitation": "сильный снег"},
    77: {"condition": "Снежные зёрна",      "emoji": "🌨️", "precipitation": "снежные зёрна"},
    80: {"condition": "Ливень",             "emoji": "🌧️", "precipitation": "слабый ливень"},
    81: {"condition": "Ливень",             "emoji": "🌧️", "precipitation": "ливень"},
    82: {"condition": "Ливень",             "emoji": "⛈️",  "precipitation": "сильный ливень"},
    85: {"condition": "Снегопад",           "emoji": "🌨️", "precipitation": "слабый снегопад"},
    86: {"condition": "Снегопад",           "emoji": "❄️",  "precipitation": "сильный снегопад"},
    95: {"condition": "Гроза",              "emoji": "⛈️",  "precipitation": "гроза"},
    96: {"condition": "Гроза с градом",     "emoji": "⛈️",  "precipitation": "гроза с градом"},
    99: {"condition": "Гроза с градом",     "emoji": "⛈️",  "precipitation": "сильная гроза с градом"},
}


def get_weather_info(code: int) -> Dict[str, Any]:
    return WMO_WEATHER_CODES.get(code, {
        "condition": "Неизвестно",
        "emoji": "🌡️",
        "precipitation": None
    })


def with_retry(max_retries: int = MAX_RETRIES,
               delay_base: float = RETRY_DELAY_BASE,
               delay_max: float = RETRY_DELAY_MAX):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except aiohttp.ClientError as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = min(delay_base * (2 ** attempt), delay_max)
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                            f"after {delay:.1f}s: {e}"
                        )
                        await asyncio.sleep(delay)
                except Exception:
                    raise
            logger.error(f"All {max_retries} retries failed for {func.__name__}")
            raise last_exception if last_exception else Exception("Unknown error")
        return wrapper
    return decorator


class APIClient:
    """Асинхронный клиент: погода, криптовалюты, курсы валют."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=REQUEST_TIMEOUT,
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _validate_coords(lat: float, lon: float) -> bool:
        return -90 <= lat <= 90 and -180 <= lon <= 180

    # === ПОГОДА ===
    @with_retry(max_retries=2)
    async def fetch_weather(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        if not _weather_metrics.can_make_request():
            return None
        if not self._validate_coords(lat, lon):
            logger.warning(f"Invalid coords: {lat}, {lon}")
            return None
        try:
            session = await self._get_session()
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,weather_code,cloud_cover,precipitation",
                "timezone": "auto"
            }
            _weather_metrics.increment()
            async with session.get(f"{OPEN_METEO_BASE}/forecast", params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                current = data.get("current", {})
                weather_code = current.get("weather_code", 0)
                info = get_weather_info(weather_code)
                return {
                    "temperature":      current.get("temperature_2m"),
                    "humidity":         current.get("relative_humidity_2m"),
                    "weather_code":     weather_code,
                    "cloud_cover":      current.get("cloud_cover", 0),
                    "precipitation":    current.get("precipitation", 0),
                    "condition":        info["condition"],
                    "condition_emoji":  info["emoji"],
                    "precipitation_type": info["precipitation"],
                    "time":             current.get("time", ""),
                }
        except aiohttp.ClientError as e:
            logger.error(f"Weather error: {e}")
            return None
        except Exception as e:
            logger.error(f"Weather unexpected error: {e}")
            return None

    def get_weather_remaining_requests(self) -> int:
        return _weather_metrics.get_remaining()

    # === КРИПТОВАЛЮТЫ ===
    @with_retry(max_retries=3)
    async def fetch_crypto_prices(self) -> Optional[Dict[str, Any]]:
        try:
            session = await self._get_session()
            async with session.get(COINGECKO_URL, params=COINGECKO_PARAMS) as resp:
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientError as e:
            logger.error(f"CoinGecko error: {e}")
            return None
        except Exception as e:
            logger.error(f"Crypto unexpected error: {e}")
            return None

    # === ВАЛЮТЫ ===
    @with_retry(max_retries=3)
    async def fetch_fiat_rates(self) -> Optional[Dict[str, Any]]:
        try:
            session = await self._get_session()
            async with session.get(EXCHANGE_RATE_URL) as resp:
                resp.raise_for_status()
                data = await resp.json()
                rates = data.get("rates", {})

                def rub_per(code: str) -> Optional[float]:
                    rate = rates.get(code)
                    return round(1 / rate, 2) if rate else None

                return {
                    "date": datetime.fromtimestamp(
                        data.get("time_last_updated", 0), tz=timezone.utc
                    ).strftime('%d.%m.%Y'),
                    "base": "RUB",
                    "rates": {
                        "USD": rub_per("USD"),
                        "EUR": rub_per("EUR"),
                        "CNY": rub_per("CNY"),
                    }
                }
        except aiohttp.ClientError as e:
            logger.error(f"ExchangeRate error: {e}")
            return None
        except Exception as e:
            logger.error(f"Fiat unexpected error: {e}")
            return None

    # === ОБЩЕЕ ОБНОВЛЕНИЕ (без новостей) ===
    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Загружает крипту и курсы валют параллельно.
        Новости — отдельно через NewsDigest.refresh_all().
        """
        results = await asyncio.gather(
            self.fetch_crypto_prices(),
            self.fetch_fiat_rates(),
            return_exceptions=True
        )

        def safe(r, name):
            if isinstance(r, Exception):
                logger.warning(f"{name} failed: {r}")
                return None
            return r

        data = {
            "crypto":     safe(results[0], "crypto"),
            "fiat":       safe(results[1], "fiat"),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }
        ok = sum(1 for v in [data["crypto"], data["fiat"]] if v)
        logger.info(f"fetch_all_data: {ok}/2 sources ok")
        return data