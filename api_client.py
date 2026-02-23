"""
Клиент для внешних API
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any, Callable
from functools import wraps
from dataclasses import dataclass

import aiohttp

from config import (
    OPEN_METEO_BASE, COINGECKO_URL, COINGECKO_PARAMS,
    EXCHANGE_RATE_URL, NEWS_SOURCES
)

logger = logging.getLogger(__name__)
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)

# === RETRY CONFIGURATION ===
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1.0  # Базовая задержка в секундах
RETRY_DELAY_MAX = 10.0  # Максимальная задержка

# === WEATHER API RATE LIMIT ===
WEATHER_HOURLY_LIMIT = 10  # Запросов в час


@dataclass
class WeatherMetrics:
    """Метрики запросов погоды"""
    hourly_calls: int = 0
    last_hour_reset: float = 0
    
    def can_make_request(self) -> bool:
        """Проверка лимита запросов"""
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
        """Увеличение счётчика"""
        self.hourly_calls += 1
    
    def get_remaining(self) -> int:
        """Остаток запросов"""
        self.can_make_request()  # Сброс если новый час
        return WEATHER_HOURLY_LIMIT - self.hourly_calls


# Глобальные метрики погоды
_weather_metrics = WeatherMetrics()


# === WMO WEATHER CODES ===
# https://open-meteo.com/en/docs
WMO_WEATHER_CODES = {
    0: {"condition": "Ясно", "emoji": "☀️", "precipitation": None},
    1: {"condition": "Малооблачно", "emoji": "🌤️", "precipitation": None},
    2: {"condition": "Переменная облачность", "emoji": "⛅", "precipitation": None},
    3: {"condition": "Облачно", "emoji": "☁️", "precipitation": None},
    45: {"condition": "Туман", "emoji": "🌫️", "precipitation": "туман"},
    48: {"condition": "Изморозь", "emoji": "🌫️", "precipitation": "изморозь"},
    51: {"condition": "Морось", "emoji": "🌦️", "precipitation": "слабая морось"},
    53: {"condition": "Морось", "emoji": "🌧️", "precipitation": "морось"},
    55: {"condition": "Морось", "emoji": "🌧️", "precipitation": "сильная морось"},
    61: {"condition": "Дождь", "emoji": "🌧️", "precipitation": "слабый дождь"},
    63: {"condition": "Дождь", "emoji": "🌧️", "precipitation": "дождь"},
    65: {"condition": "Дождь", "emoji": "🌧️", "precipitation": "сильный дождь"},
    66: {"condition": "Ледяной дождь", "emoji": "🌨️", "precipitation": "ледяной дождь"},
    67: {"condition": "Ледяной дождь", "emoji": "🌨️", "precipitation": "сильный ледяной дождь"},
    71: {"condition": "Снег", "emoji": "🌨️", "precipitation": "слабый снег"},
    73: {"condition": "Снег", "emoji": "❄️", "precipitation": "снег"},
    75: {"condition": "Снег", "emoji": "❄️", "precipitation": "сильный снег"},
    77: {"condition": "Снежные зёрна", "emoji": "🌨️", "precipitation": "снежные зёрна"},
    80: {"condition": "Ливень", "emoji": "🌧️", "precipitation": "слабый ливень"},
    81: {"condition": "Ливень", "emoji": "🌧️", "precipitation": "ливень"},
    82: {"condition": "Ливень", "emoji": "⛈️", "precipitation": "сильный ливень"},
    85: {"condition": "Снегопад", "emoji": "🌨️", "precipitation": "слабый снегопад"},
    86: {"condition": "Снегопад", "emoji": "❄️", "precipitation": "сильный снегопад"},
    95: {"condition": "Гроза", "emoji": "⛈️", "precipitation": "гроза"},
    96: {"condition": "Гроза с градом", "emoji": "⛈️", "precipitation": "гроза с градом"},
    99: {"condition": "Гроза с градом", "emoji": "⛈️", "precipitation": "сильная гроза с градом"},
}


def get_weather_info(code: int) -> Dict[str, Any]:
    """Получить информацию о погоде по WMO коду"""
    return WMO_WEATHER_CODES.get(code, {
        "condition": "Неизвестно",
        "emoji": "🌡️",
        "precipitation": None
    })


def with_retry(max_retries: int = MAX_RETRIES, 
               delay_base: float = RETRY_DELAY_BASE,
               delay_max: float = RETRY_DELAY_MAX):
    """
    Декоратор для ретраев асинхронных функций с экспоненциальной задержкой.
    """
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
                            f"after {delay:.1f}s. Error: {e}"
                        )
                        await asyncio.sleep(delay)
                except Exception as e:
                    # Для других исключений не делаем ретраи
                    raise
            
            logger.error(f"All {max_retries} retries failed for {func.__name__}")
            raise last_exception if last_exception else Exception("Unknown error")
        
        return wrapper
    return decorator


class APIClient:
    """Асинхронный клиент для бесплатных API"""
    
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
            logger.debug("HTTP session closed")

    @staticmethod
    def _validate_coords(lat: float, lon: float) -> bool:
        """Валидация координат"""
        return -90 <= lat <= 90 and -180 <= lon <= 180

    # === ПОГОДА: Open-Meteo ===
    @with_retry(max_retries=2)
    async def fetch_weather(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Получить текущую погоду по координатам.
        Лимит: 10 запросов в час.
        """
        # Проверка лимита
        if not _weather_metrics.can_make_request():
            logger.warning("Weather API: Rate limit exceeded")
            return None
        
        if not self._validate_coords(lat, lon):
            logger.warning(f"Invalid coordinates: lat={lat}, lon={lon}")
            return None
        
        try:
            session = await self._get_session()
            
            # Запрашиваем расширенные данные
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,weather_code,cloud_cover,precipitation",
                "timezone": "auto"
            }
            
            _weather_metrics.increment()
            logger.info(
                f"Weather API: Request {_weather_metrics.hourly_calls}/{WEATHER_HOURLY_LIMIT} "
                f"for lat={lat:.2f}, lon={lon:.2f}"
            )
            
            async with session.get(f"{OPEN_METEO_BASE}/forecast", params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                current = data.get("current", {})
                
                weather_code = current.get("weather_code", 0)
                weather_info = get_weather_info(weather_code)
                
                # Форматируем ответ
                return {
                    "temperature": current.get("temperature_2m"),
                    "humidity": current.get("relative_humidity_2m"),
                    "weather_code": weather_code,
                    "cloud_cover": current.get("cloud_cover", 0),
                    "precipitation": current.get("precipitation", 0),
                    "condition": weather_info["condition"],
                    "condition_emoji": weather_info["emoji"],
                    "precipitation_type": weather_info["precipitation"],
                    "time": current.get("time", "")
                }
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка погоды Open-Meteo: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка погоды: {e}")
            return None
    
    def get_weather_remaining_requests(self) -> int:
        """Получить остаток запросов погоды"""
        return _weather_metrics.get_remaining()

    # === КРИПТОВАЛЮТЫ: CoinGecko ===
    @with_retry(max_retries=3)
    async def fetch_crypto_prices(self) -> Optional[Dict[str, Any]]:
        """Получить цены криптовалют"""
        try:
            session = await self._get_session()
            async with session.get(COINGECKO_URL, params=COINGECKO_PARAMS) as resp:
                resp.raise_for_status()
                data = await resp.json()
                logger.debug(f"Fetched crypto prices for {len(data)} coins")
                return data
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка CoinGecko: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка крипто: {e}")
            return None

    # === ФИАТ ВАЛЮТЫ: ExchangeRate-API ===
    @with_retry(max_retries=3)
    async def fetch_fiat_rates(self) -> Optional[Dict[str, Any]]:
        """Получить курсы USD, EUR, CNY к рублю"""
        try:
            session = await self._get_session()
            async with session.get(EXCHANGE_RATE_URL) as resp:
                resp.raise_for_status()
                data = await resp.json()

                # Базовая валюта RUB → rates показывают цену 1 RUB в иностранной валюте
                # Нам нужно обратное: сколько рублей стоит 1 единица иностранной валюты
                rates = data.get("rates", {})

                def rub_per(code: str) -> Optional[float]:
                    rate = rates.get(code)
                    return round(1 / rate, 2) if rate else None

                result = {
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
                logger.debug(f"Fetched fiat rates: USD={result['rates']['USD']}")
                return result
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка ExchangeRate-API: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка курсов: {e}")
            return None

    # === НОВОСТИ: Прямой парсинг RSS/XML ===
    async def fetch_news(self, max_items_per_source: int = 3) -> Optional[list]:
        """Получить новости из RSS-лент"""
        all_news = []
        session = await self._get_session()
        
        for rss_url in NEWS_SOURCES:
            try:
                async with session.get(rss_url) as resp:
                    resp.raise_for_status()
                    xml_text = await resp.text()
                    
                    # Парсим XML
                    root = ET.fromstring(xml_text)
                    channel_title = root.findtext('.//channel/title', default="Источник")
                    
                    for item in root.findall('.//item')[:max_items_per_source]:
                        title = item.findtext('title', default="Без заголовка")
                        link = item.findtext('link', default="#")
                        pub_date = item.findtext('pubDate', default="")
                        
                        all_news.append({
                            "title": title[:200] if title else "Без заголовка",
                            "link": link,
                            "pub_date": pub_date,
                            "source": channel_title
                        })
                        
            except aiohttp.ClientError as e:
                logger.warning(f"Ошибка загрузки новостей из {rss_url}: {e}")
                continue
            except ET.ParseError as e:
                logger.warning(f"Ошибка парсинга XML из {rss_url}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Неожиданная ошибка новостей {rss_url}: {e}")
                continue
        
        if all_news:
            # Сортируем по дате (новые первыми)
            all_news.sort(key=lambda x: x.get("pub_date", ""), reverse=True)
            logger.debug(f"Fetched {len(all_news)} news items")
            return all_news
        
        logger.warning("No news fetched from any source")
        return None

    # === УТИЛИТА: Массовая загрузка ===
    async def fetch_all_data(self) -> Dict[str, Any]:
        """
        Загружает глобальные данные (крипта, фиат, новости) параллельно.
        Погода загружается отдельно по координатам.
        """
        tasks = [
            self.fetch_crypto_prices(),
            self.fetch_fiat_rates(),
            self.fetch_news(),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)

        def safe_extract(result: Any, name: str) -> Any:
            if isinstance(result, Exception):
                logger.warning(f"{name} failed: {result}")
                return None
            return result

        data = {
            "crypto": safe_extract(results[0], "crypto"),
            "fiat": safe_extract(results[1], "fiat"),
            "news": safe_extract(results[2], "news"),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }

        # Логируем успешность
        success_count = sum(1 for v in [data["crypto"], data["fiat"], data["news"]] if v)
        logger.info(f"Fetched {success_count}/3 data sources successfully")
        
        return data