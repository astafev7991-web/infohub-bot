"""
Основной файл Telegram-бота «ИнфоХаб»
Рефакторинг: класс BotApp, rate limiting, обработка ошибок, новые команды
Reply-клавиатуры вместо inline
"""
import asyncio
import logging
import sys
import traceback
import html
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import (
    BOT_TOKEN, LOG_LEVEL, DAILY_BROADCAST_HOUR, DAILY_BROADCAST_MINUTE,
    CATEGORIES, CITY_COORDINATES, DEFAULT_LAT, DEFAULT_LON,
    PREMIUM_PROMO_TEXT, DONATE_BUTTON_URL, DB_PATH, CACHE_PATH,
    RATE_LIMIT_SECONDS, MARKET_CACHE_PATH, NEWS_CACHE_PATH, ADMIN_ID
)
from database import Database
from cache_manager import CacheManager
from api_client import APIClient
from market_digest import MarketDigest
from news_digest import NewsDigest, NEWS_CATEGORIES
from utils.decorators import (
    rate_limit, handle_telegram_errors, track_usage, get_usage_stats
)

# === LOGGING SETUP ===
import io

# Исправление кодировки для Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger(__name__)


class BotApp:
    """Главный класс приложения бота."""
    
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.db: Optional[Database] = None
        self.api_client: Optional[APIClient] = None
        self.cache_manager: Optional[CacheManager] = None
        self.market_digest: Optional[MarketDigest] = None
        self.news_digest: Optional[NewsDigest] = None
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.keyboards: Dict[str, Any] = {}
        self._shutdown_requested: bool = False
        self._user_state: Dict[int, str] = {}  # Состояния пользователей

    # === KEYBOARD BUILDERS (REPLY) ===
    def _create_keyboards(self) -> Dict[str, Any]:
        """Создание reply-клавиатур"""
        
        # Главное меню
        def get_main_keyboard():
             return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📊 Мой дайджест")],
                    [KeyboardButton(text="💰 Крипто-дайджест"), KeyboardButton(text="📰 Новости")],
                    [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="🌍 Сменить город")],
                ],
                resize_keyboard=True,
                one_time_keyboard=False
            )

        # Меню настроек
        def get_settings_keyboard(user_prefs: dict):
            buttons = []
            row = []
            for cat_key, cat_name in CATEGORIES.items():
                status = "✅" if user_prefs.get(cat_key, True) else "❌"
                row.append(KeyboardButton(text=f"{status} {cat_name}"))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([KeyboardButton(text="🔙 Назад в меню")])
            
            return ReplyKeyboardMarkup(
                keyboard=buttons,
                resize_keyboard=True
            )

        # Меню выбора города
        def get_city_keyboard():
            buttons = []
            row = []
            for city in list(CITY_COORDINATES.keys())[:8]:  # Первые 8 городов
                row.append(KeyboardButton(text=city.title()))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([KeyboardButton(text="🔙 Назад в меню")])
            
            return ReplyKeyboardMarkup(
                keyboard=buttons,
                resize_keyboard=True
            )
        
        # Меню новостей (NewsData.io — русский язык)
        def get_news_keyboard():
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📰 Главное"), KeyboardButton(text="🌍 В мире")],
                    [KeyboardButton(text="💻 Технологии"), KeyboardButton(text="💼 Бизнес")],
                    [KeyboardButton(text="🔬 Наука"), KeyboardButton(text="🏛️ Политика")],
                    [KeyboardButton(text="📰 Все новости")],
                    [KeyboardButton(text="🔙 Назад в меню")],
                ],
                resize_keyboard=True
            )
        
        # Меню крипто
        def get_crypto_keyboard():
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🔄 Обновить крипто"), KeyboardButton(text="📊 Метрики API")],
                    [KeyboardButton(text="🔙 Назад в меню")],
                ],
                resize_keyboard=True
            )
        
        return {
            "main": get_main_keyboard,
            "settings": get_settings_keyboard,
            "city": get_city_keyboard,
            "news": get_news_keyboard,
            "crypto": get_crypto_keyboard,
        }

    @track_usage("start")
    @handle_telegram_errors
    async def cmd_start(self, message: types.Message):
        """Обработчик команды /start"""
        user = message.from_user
        
        try:
            await self.db.add_user(user.id, user.username, user.first_name)
            logger.info(f"👤 Пользователь: {user.id} (@{user.username})")
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user.id}: {e}")

        # Сбрасываем состояние
        self._user_state[user.id] = "main"

        welcome_text = (
            f"👋 Привет, {html.escape(user.first_name or 'друг')}!\n\n"
            f"Я — <b>ИнфоХаб</b>, твой персональный агрегатор.\n\n"
            f"🔹 Дайджест раз в день в {DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d} MSK\n"
            f"🔹 Настраивай категории под себя\n"
            f"🔹 0 рублей затрат!\n\n"
            f"Используй кнопки меню ниже 👇\n\n"
            f"📝 /help — справка по командам"
        )
        await message.answer(
            welcome_text, 
            parse_mode="HTML",
            reply_markup=self.keyboards["main"]()
        )
        
    @track_usage("help")
    @handle_telegram_errors
    async def cmd_help(self, message: types.Message):
        """Обработчик команды /help"""
        help_text = """
📖 <b>Справка по боту ИнфоХаб</b>

<b>Команды:</b>
/start — Запуск бота
/help — Эта справка
/ping — Проверка работоспособности
/stats — Статистика бота

<b>Кнопки меню:</b>
📊 <b>Мой дайджест</b> — Получить сводку сейчас
💰 <b>Крипто-дайджест</b> — Сводка крипторынка
📰 <b>Новости</b> — Новости на русском (NewsData.io)
⚙️ <b>Настройки</b> — Выбрать категории
🌍 <b>Сменить город</b> — Указать свой город

<b>Категории дайджеста:</b>
• 🌤 Погода • 💰 Криптовалюты
• 💱 Курсы валют • 📰 Новости

<b>Время рассылки:</b>
Ежедневно в 09:00 по Москве
"""
        await message.answer(help_text, parse_mode="HTML")

    @track_usage("ping")
    @handle_telegram_errors
    async def cmd_ping(self, message: types.Message):
        """Проверка работоспособности"""
        await message.answer("🏓 Pong! Бот работает.")

    @track_usage("stats")
    @handle_telegram_errors
    async def cmd_stats(self, message: types.Message):
        """Статистика бота"""
        try:
            user_count = await self.db.get_user_count()
            premium_count = await self.db.get_premium_user_count()
            stats = get_usage_stats()
            
            stats_text = (
                f"📊 <b>Статистика бота</b>\n\n"
                f"👥 Пользователей: {user_count}\n"
                f"💎 Премиум: {premium_count}\n"
                f"📁 Городов: {len(CITY_COORDINATES)}\n\n"
                f"📈 <b>Использование:</b>\n"
            )
            for action, count in stats.items():
                stats_text += f"  • {action}: {count}\n"
            
            await message.answer(stats_text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            await message.answer("❌ Ошибка получения статистики")

    @handle_telegram_errors
    async def cmd_api_metrics(self, message: types.Message):
        """Метрики API (только для админа)"""
        user_id = message.from_user.id
        
        if not ADMIN_ID or user_id != ADMIN_ID:
            await message.answer("⛔ Команда доступна только администратору")
            return
        
        try:
            lines = ["📊 <b>Метрики API</b>\n"]
            
            # Погода (Open-Meteo)
            weather_remaining = self.api_client.get_weather_remaining_requests()
            lines.append(f"🌤 <b>Open-Meteo (погода):</b>")
            lines.append(f"   Осталось: {weather_remaining}/10 в час")
            lines.append("")
            
            # Новости (NewsData.io)
            if self.news_digest:
                news_metrics = self.news_digest.get_metrics()
                lines.append(f"📰 <b>NewsData.io (новости):</b>")
                lines.append(f"   В час: {news_metrics['hourly_remaining']}/{news_metrics['hourly_limit']}")
                lines.append(f"   В день: {news_metrics['daily_remaining']}/{news_metrics['daily_limit']}")
                lines.append(f"   Всего: {news_metrics['total_calls']}")
                lines.append(f"   Кэш: {news_metrics['cache_entries']} записей")
                lines.append("")
            
            # Крипто (CoinGecko)
            if self.market_digest:
                market_metrics = self.market_digest.get_metrics()
                lines.append(f"💰 <b>CoinGecko (крипто):</b>")
                lines.append(f"   Запросов/мин: {market_metrics['coingecko_calls_last_minute']}/30")
                lines.append(f"   Кэш: {market_metrics['cache_entries']} записей")
                lines.append("")
            
            await message.answer("\n".join(lines), parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Error getting API metrics: {e}")
            await message.answer("❌ Ошибка получения метрик")

    @handle_telegram_errors
    async def handle_button(self, message: types.Message):
        """Обработка нажатий на reply-кнопки"""
        user_id = message.from_user.id
        text = message.text
        state = self._user_state.get(user_id, "main")

        # === ГЛАВНОЕ МЕНЮ ===
        if text == "📊 Мой дайджест":
            await self._send_digest_now(message)
        
        elif text == "💰 Крипто-дайджест":
            await self._show_crypto_digest(message)
        
        elif text == "📰 Новости":
            self._user_state[user_id] = "news"
            await message.answer(
                "📰 <b>Новости</b>\n\nВыберите категорию:",
                parse_mode="HTML",
                reply_markup=self.keyboards["news"]()
            )

        elif text == "⚙️ Настройки":
            self._user_state[user_id] = "settings"
            prefs = await self.db.get_user_preferences(user_id)
            await message.answer(
                "⚙️ <b>Настройки категорий</b>\n\nНажмите на категорию, чтобы включить/выключить:",
                parse_mode="HTML",
                reply_markup=self.keyboards["settings"](prefs)
            )
        
        elif text == "🌍 Сменить город":
            self._user_state[user_id] = "city"
            await message.answer(
                "🌍 <b>Выберите город:</b>",
                parse_mode="HTML",
                reply_markup=self.keyboards["city"]()
            )
        
        elif text == "🔙 Назад в меню":
            self._user_state[user_id] = "main"
            await message.answer(
                "🏠 <b>Главное меню</b>",
                parse_mode="HTML",
                reply_markup=self.keyboards["main"]()
            )
        
        # === НАСТРОЙКИ ===
        elif state == "settings":
            await self._handle_settings_button(message)
        
        # === ГОРОД ===
        elif state == "city":
            await self._handle_city_button(message)
        
        # === НОВОСТИ ===
        elif state == "news":
            await self._handle_news_button(message)
        
        # === КРИПТО ===
        elif state == "crypto":
            await self._handle_crypto_button(message)
        
        else:
            # Неизвестная кнопка — возвращаем в главное меню
            self._user_state[user_id] = "main"
            await message.answer(
                "🤔 Не понимаю. Используйте кнопки меню.",
                reply_markup=self.keyboards["main"]()
            )

    # === SETTINGS HANDLERS ===
    async def _handle_settings_button(self, message: types.Message):
        """Обработка кнопок настроек"""
        user_id = message.from_user.id
        text = message.text
        
        # Проверяем, это кнопка категории или "Назад"
        if text == "🔙 Назад в меню":
            self._user_state[user_id] = "main"
            await message.answer(
                "🏠 Главное меню",
                reply_markup=self.keyboards["main"]()
            )
            return
        
        # Ищем категорию в тексте кнопки
        for cat_key, cat_name in CATEGORIES.items():
            if cat_name in text or cat_key in text.lower():
                prefs = await self.db.get_user_preferences(user_id)
                new_state = not prefs.get(cat_key, True)
                await self.db.toggle_preference(user_id, cat_key, new_state)
                
                # Обновляем клавиатуру
                prefs = await self.db.get_user_preferences(user_id)
                status = "включена" if new_state else "выключена"
                await message.answer(
                    f"✅ {cat_name}: {status}",
                    reply_markup=self.keyboards["settings"](prefs)
                )
                return
        
        await message.answer("🤔 Неизвестная категория")

    # === CITY HANDLERS ===
    async def _handle_city_button(self, message: types.Message):
        """Обработка кнопок города"""
        user_id = message.from_user.id
        text = message.text
        
        if text == "🔙 Назад в меню":
            self._user_state[user_id] = "main"
            await message.answer(
                "🏠 Главное меню",
                reply_markup=self.keyboards["main"]()
            )
            return
        
        # Ищем город в тексте
        city_name = text.lower().strip()
        if city_name in CITY_COORDINATES:
            try:
                await self.db.update_city(user_id, city_name)
                self._user_state[user_id] = "main"
                await message.answer(
                    f"✅ Город изменён на <b>{city_name.title()}</b>",
                    parse_mode="HTML",
                    reply_markup=self.keyboards["main"]()
                )
            except Exception as e:
                logger.error(f"Ошибка обновления города: {e}")
                await message.answer("❌ Ошибка сохранения города")
        else:
            await message.answer("❌ Город не найден. Выберите из списка.")

    # === NEWS HANDLERS ===
    async def _handle_news_button(self, message: types.Message):
        """Обработка кнопок новостей"""
        user_id = message.from_user.id
        text = message.text
        
        if text == "🔙 Назад в меню":
            self._user_state[user_id] = "main"
            await message.answer(
                "🏠 Главное меню",
                reply_markup=self.keyboards["main"]()
            )
            return
        
        if not self.news_digest:
            await message.answer("❌ Сервис новостей недоступен")
            return
        
        # Категории NewsData.io (язык: русский)
        category = "top"
        
        if text == "📰 Главное":
            category = "top"
        elif text == "🌍 В мире":
            category = "world"
        elif text == "💻 Технологии":
            category = "technology"
        elif text == "💼 Бизнес":
            category = "business"
        elif text == "🔬 Наука":
            category = "science"
        elif text == "🏛️ Политика":
            category = "politics"
        elif text == "📰 Все новости":
            digest_text = self.news_digest.get_combined_digest(max_per_category=3)
            await message.answer(
                digest_text,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            return
        else:
            return
        
        digest_text = self.news_digest.get_news_digest(
            language="ru",
            category=category,
            max_items=5
        )
        await message.answer(
            digest_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )

    # === CRYPTO HANDLERS ===
    async def _show_crypto_digest(self, message: types.Message):
        """Показать крипто-дайджест"""
        user_id = message.from_user.id
        self._user_state[user_id] = "crypto"
        
        if not self.market_digest:
            await message.answer("❌ Сервис крипто-дайджеста недоступен")
            return
        
        digest_text = self.market_digest.get_digest()
        await message.answer(
            digest_text,
            parse_mode="HTML",
            reply_markup=self.keyboards["crypto"]()
        )

    async def _handle_crypto_button(self, message: types.Message):
        """Обработка кнопок крипто"""
        user_id = message.from_user.id
        text = message.text
        
        if text == "🔙 Назад в меню":
            self._user_state[user_id] = "main"
            await message.answer(
                "🏠 Главное меню",
                reply_markup=self.keyboards["main"]()
            )
            return
        
        if not self.market_digest:
            await message.answer("❌ Сервис недоступен")
            return
        
        if text == "🔄 Обновить крипто":
            # Триггерим фоновое обновление
            asyncio.create_task(self.market_digest.refresh_all())
            await asyncio.sleep(1)  # Ждём немного
            digest_text = self.market_digest.get_digest()
            await message.answer(
                digest_text,
                parse_mode="HTML",
                reply_markup=self.keyboards["crypto"]()
            )
        
        elif text == "📊 Метрики API":
            metrics = self.market_digest.get_metrics()
            
            text_msg = (
                f"📊 <b>Метрики API</b>\n\n"
                f"🔴 CoinGecko (минута): <b>{metrics['coingecko_calls_last_minute']}/30</b>\n"
                f"🟡 Fear & Greed (минута): <b>{metrics['fng_calls_last_minute']}</b>\n\n"
                f"📁 Записей в кэше: <b>{metrics['cache_entries']}</b>\n\n"
            )
            
            for key, status in metrics.get("cache_status", {}).items():
                emoji = "✅" if status.get("valid") else "⏰"
                stale = " ⚠️" if status.get("is_stale") else ""
                age = status.get("age_seconds", 0)
                text_msg += f"{emoji} {key}: {age}s{stale}\n"
            
            await message.answer(text_msg, parse_mode="HTML")

    # === DIGEST ===
    @rate_limit(seconds=RATE_LIMIT_SECONDS)
    @track_usage("digest")
    @handle_telegram_errors
    async def _send_digest_now(self, message: types.Message):
        """Отправить дайджест"""
        user_id = message.from_user.id
        
        try:
            user_city = await self.db.get_user_city(user_id)
            lat, lon = CITY_COORDINATES.get(user_city, (DEFAULT_LAT, DEFAULT_LON))
            
            data = await self.cache_manager.get_data(lat, lon)
            prefs = await self.db.get_user_preferences(user_id)
            message_text = await self._format_digest(data, prefs, user_city)
            
            await message.answer(
                message_text,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            logger.info(f"📬 Дайджест отправлен пользователю {user_id}")
        except Exception as e:
            logger.error(f"Ошибка дайджеста для {user_id}: {e}")
            await message.answer(
                "⚠️ Временно недоступно\n\nПопробуйте позже."
            )

    async def _format_digest(self, cache_data: dict, prefs: dict, city: str) -> str:
        """Форматирование дайджеста"""
        msk = timezone(timedelta(hours=3))
        current_time = datetime.now(msk).strftime('%d.%m %H:%M')
        parts = [f"🗞 <b>ИнфоХаб</b> • {current_time} MSK • {html.escape(city.title())}"]

        # Погода
        if prefs.get("weather") and cache_data.get("weather"):
            w = cache_data["weather"]
            temp = w.get('temperature', 'N/A')
            condition = w.get('condition', 'Неизвестно')
            emoji = w.get('condition_emoji', '🌡️')
            precip_type = w.get('precipitation_type')
            cloud_cover = w.get('cloud_cover', 0)
            
            # Формируем строку осадков
            precip_str = ""
            if precip_type:
                precip_str = f", {precip_type}"
            elif cloud_cover < 20:
                precip_str = ", без осадков"
            
            parts.append(f"\n{emoji} <b>Погода:</b> {temp}°C, {condition}{precip_str}")
        elif prefs.get("weather"):
            parts.append("\n🌡️ <b>Погода:</b> временно недоступно")

        # Криптовалюты
        if prefs.get("crypto") and cache_data.get("crypto"):
            crypto = cache_data["crypto"]
            parts.append("\n💰 <b>Крипто:</b>")
            for coin in ["bitcoin", "ethereum", "tether"]:
                if coin in crypto:
                    c = crypto[coin]
                    usd = c.get("usd", 0)
                    change = c.get("usd_24h_change", 0)
                    sign = "🟢" if change >= 0 else "🔴"
                    parts.append(f"  {sign} {coin.title()}: ${usd:,.2f} ({change:+.1f}%)")

        # Курсы валют
        if prefs.get("fiat") and cache_data.get("fiat"):
            fiat = cache_data["fiat"]
            rates = fiat.get("rates", {})
            parts.append(f"\n💱 <b>Курсы к рублю</b> ({fiat.get('date', 'N/A')}):")
            
            pairs = [("USD", "🇺🇸 Доллар"), ("EUR", "🇪🇺 Евро"), ("CNY", "🇨🇳 Юань")]
            for code, name in pairs:
                val = rates.get(code)
                parts.append(f"  {name}: {val} ₽" if val else f"  {name}: недоступно")

        # Новости (из NewsData.io вместо RSS)
        if prefs.get("news") and self.news_digest:
            news_articles = self.news_digest.get_cached_articles(
                language="ru", category="top", max_items=3
            )
            if news_articles:
                parts.append("\n📰 <b>Новости:</b>")
                for item in news_articles:
                    raw_title = item.get("title", "Без заголовка")
                    title = html.escape((raw_title[:60] + "...") if len(raw_title) > 60 else raw_title)
                    source = html.escape(item.get("source", "Источник"))
                    link = item.get("url", "#")
                    parts.append(f" • <a href='{link}'>{title}</a> <i>({source})</i>")
            else:
                parts.append("\n📰 <b>Новости:</b> временно недоступно")
        elif prefs.get("news"):
            parts.append("\n📰 <b>Новости:</b> временно недоступно")
        
        parts.append(f"\n\n{PREMIUM_PROMO_TEXT}")
        return "\n".join(parts)

    # === BROADCAST ===
    async def daily_broadcast(self):
        """Ежедневная рассылка"""
        logger.info("🚀 Запуск ежедневной рассылки")
        
        try:
            await self.cache_manager.force_refresh()
            
            users = await self.db.get_all_active_users()
            logger.info(f"📬 Рассылка для {len(users)} пользователей")
            
            sent = 0
            failed = 0

            for user in users:
                if self._shutdown_requested:
                    logger.info("🛑 Рассылка прервана")
                    break
                
                try:
                    user_city = user.get("city", "москва")
                    lat, lon = CITY_COORDINATES.get(user_city, (DEFAULT_LAT, DEFAULT_LON))
                    
                    user_data = await self.cache_manager.get_data(lat, lon)
                    message_text = await self._format_digest(
                        user_data, user["preferences"], user_city
                    )
                    
                    await self.bot.send_message(
                        chat_id=user["user_id"],
                        text=message_text,
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                    sent += 1
                    await asyncio.sleep(0.05)
                    
                except TelegramBadRequest as e:
                    error_msg = str(e).lower()
                    if "bot was blocked" in error_msg:
                        logger.debug(f"User {user['user_id']} blocked the bot")
                    else:
                        logger.warning(f"Telegram error for {user['user_id']}: {e}")
                    failed += 1
                except Exception as e:
                    logger.error(f"Ошибка отправки {user['user_id']}: {e}")
                    failed += 1

            logger.info(f"✅ Рассылка: {sent} доставлено, {failed} ошибок")
            
        except Exception as e:
            logger.error(f"Критическая ошибка рассылки: {e}", exc_info=True)

    # === SCHEDULER ===
    def _setup_scheduler(self):
        """Настройка планировщика"""
        msk_tz = timezone(timedelta(hours=3), name="MSK")
        self.scheduler = AsyncIOScheduler(timezone=msk_tz)

        self.scheduler.add_job(
            self.daily_broadcast,
            trigger="cron",
            hour=DAILY_BROADCAST_HOUR,
            minute=DAILY_BROADCAST_MINUTE,
            id="daily_digest",
            misfire_grace_time=3600
        )
        
        self.scheduler.add_job(
            lambda: asyncio.create_task(self.cache_manager.force_refresh()),
            trigger="interval",
            minutes=30,
            id="cache_refresh",
            misfire_grace_time=300
        )
        
        if self.market_digest:
            self.scheduler.add_job(
                lambda: asyncio.create_task(self.market_digest.refresh_all()),
                trigger="interval",
                minutes=5,
                id="market_digest_refresh",
                misfire_grace_time=120
            )
        
        if self.news_digest:
            self.scheduler.add_job(
                lambda: asyncio.create_task(self.news_digest.refresh_all()),
                trigger="interval",
                hours=1,  # 1 час для экономии API запросов (лимит 20/час)
                id="news_digest_refresh",
                misfire_grace_time=600
            )
        
        self.scheduler.start()
        logger.info(f"📅 Планировщик запущен ({DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d} MSK)")

    # === LIFECYCLE ===
    async def on_startup(self):
        """Инициализация при запуске"""
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК БОТА INFOHUB")
        logger.info("=" * 60)

        if not BOT_TOKEN or ":" not in BOT_TOKEN:
            logger.critical("🛑 BOT_TOKEN отсутствует или некорректен!")
            raise ValueError("BOT_TOKEN is missing or invalid")

        logger.info("📦 Инициализация компонентов...")

        self.db = Database(DB_PATH)
        await self.db.init()
        logger.info("✅ База данных готова")

        self.api_client = APIClient()
        await self.api_client._get_session()
        logger.info("✅ HTTP-сессия готова")

        self.cache_manager = CacheManager(CACHE_PATH, self.api_client)
        await self.cache_manager.force_refresh()
        logger.info("✅ Кэш обновлён")

        self.market_digest = MarketDigest(MARKET_CACHE_PATH)
        await self.market_digest.refresh_all()
        logger.info("✅ Крипто-дайджест обновлён")

        self.news_digest = NewsDigest(NEWS_CACHE_PATH)
        await self.news_digest.refresh_all()
        logger.info("✅ Новости обновлены")

        self.keyboards = self._create_keyboards()
        self._setup_scheduler()

        me = await self.bot.get_me()
        logger.info(f"✅ Бот авторизован: @{me.username} (ID: {me.id})")
        
        user_count = await self.db.get_user_count()
        logger.info(f"📊 Пользователей: {user_count}")
        
        logger.info("=" * 60)
        logger.info("🎯 БОТ ГОТОВ К РАБОТЕ")
        logger.info("=" * 60)

    async def on_shutdown(self):
        """Остановка бота"""
        logger.info("🛑 Остановка бота...")
        self._shutdown_requested = True
        
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        
        if self.news_digest:
            await self.news_digest.close()
        
        if self.market_digest:
            await self.market_digest.close()
        
        if self.api_client:
            await self.api_client.close()
        
        if self.bot:
            await self.bot.session.close()
        
        logger.info("✅ Бот остановлен")

    def register_handlers(self):
        """Регистрация обработчиков"""
        # Команды
        self.dp.message(CommandStart())(self.cmd_start)
        self.dp.message(Command("help"))(self.cmd_help)
        self.dp.message(Command("ping"))(self.cmd_ping)
        self.dp.message(Command("stats"))(self.cmd_stats)
        self.dp.message(Command("api"))(self.cmd_api_metrics)
        
        # Текстовые сообщения (reply-кнопки)
        self.dp.message(F.text)(self.handle_button)

    async def run(self):
        """Запуск бота"""
        self.bot = Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(parse_mode="HTML")
        )
        self.dp = Dispatcher()

        self.register_handlers()
        self.dp.startup.register(self.on_startup)
        self.dp.shutdown.register(self.on_shutdown)

        logger.info("🔄 Запуск polling...")
        
        try:
            await self.dp.start_polling(
                self.bot, 
                allowed_updates=self.dp.resolve_used_update_types()
            )
        except KeyboardInterrupt:
            logger.info("⚠️ Получен сигнал остановки (Ctrl+C)")
        except Exception as e:
            logger.critical(f"🛑 КРИТИЧЕСКАЯ ОШИБКА: {e}")
            logger.critical(traceback.format_exc())
            raise
        finally:
            if self.bot:
                await self.bot.session.close()
            logger.info("🏁 Скрипт завершён")


def check_critical_files():
    """Проверка файловой системы"""
    logger.info("🔍 Проверка файловой системы...")
    
    try:
        test_file = Path(__file__).parent / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
        logger.info("✅ Права на запись: OK")
    except (IOError, OSError, PermissionError) as e:
        logger.warning(f"⚠️ Права на запись ограничены: {e}")

    logger.info(f"{'✅' if DB_PATH.exists() else '📝'} База данных: {DB_PATH}")
    logger.info(f"{'✅' if CACHE_PATH.exists() else '📝'} Кэш: {CACHE_PATH}")
    logger.info(f"{'✅' if MARKET_CACHE_PATH.exists() else '📝'} Кэш рынка: {MARKET_CACHE_PATH}")
    logger.info(f"{'✅' if NEWS_CACHE_PATH.exists() else '📝'} Кэш новостей: {NEWS_CACHE_PATH}")


def main():
    """Точка входа"""
    logger.info("📍 Точка входа: __main__")
    logger.info(f"🐍 Python version: {sys.version}")
    
    try:
        check_critical_files()
        app = BotApp()
        asyncio.run(app.run())
    except SystemExit as e:
        logger.error(f"🛑 SystemExit: {e.code}")
        sys.exit(e.code)
    except KeyboardInterrupt:
        logger.info("⚠️ Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"🛑 Неожиданная ошибка: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
