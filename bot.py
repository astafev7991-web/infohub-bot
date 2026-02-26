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
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import (
    BOT_TOKEN, LOG_LEVEL,
    CATEGORIES, BASE_CATEGORIES, NEWS_CATEGORIES as NEWS_CATS_CONFIG,
    CITY_COORDINATES, DEFAULT_LAT, DEFAULT_LON,
    PREMIUM_PROMO_TEXT, DONATE_BUTTON_URL, DB_PATH, CACHE_PATH,
    RATE_LIMIT_SECONDS, MARKET_CACHE_PATH, NEWS_CACHE_PATH, ADMIN_ID
)
from database import Database, BROADCAST_HOURS
from cache_manager import CacheManager
from api_client import APIClient
from market_digest import MarketDigest
from news_digest import NewsDigest
from utils.decorators import (
    rate_limit, handle_telegram_errors, track_usage, get_usage_stats
)

# === LOGGING SETUP ===
import io

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
        # _user_state удалён — состояние хранится в БД (миграция v5, last_state)

    # === KEYBOARD BUILDERS (REPLY) ===
    def _create_keyboards(self) -> Dict[str, Any]:
        """Создание reply-клавиатур"""

        def get_main_keyboard():
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📊 Мой дайджест")],
                    [KeyboardButton(text="📰 Новости")],
                    [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="🌍 Сменить город")],
                ],
                resize_keyboard=True,
                one_time_keyboard=False
            )

        def get_settings_keyboard(user_prefs: dict, broadcast_hour: int = 9):
            buttons = []
            logger.debug(f"Settings keyboard prefs: {user_prefs}")

            buttons.append([KeyboardButton(text="─── Основное ───")])
            for cat_key, cat_name in BASE_CATEGORIES.items():
                is_enabled = user_prefs.get(cat_key, True)
                status = "✅" if is_enabled else "❌"
                buttons.append([KeyboardButton(text=f"{status} {cat_name}")])

            buttons.append([KeyboardButton(text="─── Новости ───")])
            news_row = []
            for cat_key, cat_name in NEWS_CATS_CONFIG.items():
                is_enabled = user_prefs.get(cat_key, True)
                status = "✅" if is_enabled else "❌"
                news_row.append(KeyboardButton(text=f"{status} {cat_name}"))
                if len(news_row) == 2:
                    buttons.append(news_row)
                    news_row = []
            if news_row:
                buttons.append(news_row)

            buttons.append([KeyboardButton(text="─── Рассылка ───")])
            time_str = f"⏰ Время: {broadcast_hour:02d}:00 МСК"
            buttons.append([KeyboardButton(text=time_str)])
            buttons.append([KeyboardButton(text="🔙 Назад в меню")])

            return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

        def get_time_keyboard(current_hour: int = 9):
            buttons = []
            row = []
            for hour in [6, 7, 8, 9, 10, 11, 12]:
                marker = "✓ " if hour == current_hour else ""
                row.append(KeyboardButton(text=f"{marker}{hour:02d}:00"))
                if len(row) == 4:
                    buttons.append(row)
                    row = []
            for hour in [18, 19, 20, 21]:
                marker = "✓ " if hour == current_hour else ""
                row.append(KeyboardButton(text=f"{marker}{hour:02d}:00"))
                if len(row) == 4:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([KeyboardButton(text="🔙 Назад в настройки")])
            return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

        def get_city_keyboard():
            buttons = []
            row = []
            for city in list(CITY_COORDINATES.keys())[:8]:
                row.append(KeyboardButton(text=city.title()))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([KeyboardButton(text="🔙 Назад в меню")])
            return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

        def get_news_keyboard():
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="💰 Крипто-дайджест")],
                    [KeyboardButton(text="📰 Главное"), KeyboardButton(text="🌍 В мире")],
                    [KeyboardButton(text="💻 Технологии"), KeyboardButton(text="💼 Бизнес")],
                    [KeyboardButton(text="🔬 Наука"), KeyboardButton(text="🏥 Здоровье")],
                    [KeyboardButton(text="⚽ Спорт"), KeyboardButton(text="🎬 Развлечения")],
                    [KeyboardButton(text="🏛️ Политика"), KeyboardButton(text="📊 Все новости")],
                    [KeyboardButton(text="🔙 Назад в меню")],
                ],
                resize_keyboard=True
            )

        def get_crypto_keyboard():
            return ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🔄 Обновить крипто")],
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
            "time": get_time_keyboard,
        }

    @track_usage("start")
    @handle_telegram_errors
    async def cmd_start(self, message: types.Message, command: CommandObject = None):
        """Обработчик команды /start с поддержкой реферальных ссылок"""
        user = message.from_user
        try:
            await self.db.add_user(user.id, user.username, user.first_name)
            logger.info(f"👤 Пользователь: {user.id} (@{user.username})")
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user.id}: {e}")

        referrer_id = None
        if command and command.args:
            args = command.args.strip()
            if args.startswith("ref_"):
                try:
                    referrer_id = int(args[4:])
                    logger.info(f"Referral: referrer={referrer_id}, new_user={user.id}")
                except ValueError:
                    logger.warning(f"Invalid referral code: {args}")

        if referrer_id:
            already_referred = await self.db.is_already_referred(user.id)
            if not already_referred and referrer_id != user.id:
                success = await self.db.add_referral(referrer_id, user.id)
                if success:
                    logger.info(f"✅ Referral registered: {referrer_id} <- {user.id}")

        await self.db.set_user_state(user.id, "main")

        welcome_text = (
            f"👋 Привет, {html.escape(user.first_name or 'друг')}!\n\n"
            f"Я — <b>ИнфоХаб</b>, твой персональный агрегатор.\n\n"
            f"🔹 Дайджест раз в день\n"
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
        help_text = """
📖 <b>Справка по боту ИнфоХаб</b>

<b>Команды:</b>
/start — Запуск бота
/help — Эта справка
/ping — Проверка работоспособности
/stats — Статистика бота

<b>Кнопки меню:</b>
📊 <b>Мой дайджест</b> — Получить сводку сейчас
📰 <b>Новости</b> — Новости, крипто-дайджест
⚙️ <b>Настройки</b> — Выбрать категории
🌍 <b>Сменить город</b> — Указать свой город

<b>Категории дайджеста:</b>
• 🌤 Погода • 💰 Криптовалюты • 💱 Курсы валют
• 📰 Новости (10 категорий на выбор)

<b>Время рассылки:</b>
Настраиваемое (утро 6-12, вечер 18-21)
"""
        await message.answer(help_text, parse_mode="HTML")

    @track_usage("ping")
    @handle_telegram_errors
    async def cmd_ping(self, message: types.Message):
        await message.answer("🏓 Pong! Бот работает.")

    @track_usage("stats")
    @handle_telegram_errors
    async def cmd_stats(self, message: types.Message):
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
            weather_remaining = self.api_client.get_weather_remaining_requests()
            lines.append(f"🌤 <b>Open-Meteo (погода):</b>")
            lines.append(f"   Осталось: {weather_remaining}/10 в час")
            lines.append("")
            if self.news_digest:
                nm = self.news_digest.get_metrics()
                lines.append(f"📰 <b>News APIs:</b>")
                if 'newsapi' in nm:
                    lines.append(f"   NewsAPI: {nm['newsapi']['daily_remaining']}/{nm['newsapi']['daily_limit']} в день")
                if 'newsdata' in nm:
                    lines.append(f"   NewsData: {nm['newsdata']['daily_remaining']}/{nm['newsdata']['daily_limit']} в день")
                lines.append(f"   Всего: {nm['total_calls']} запросов")
                lines.append(f"   Кэш: {nm['cache_entries']} записей")
                lines.append("")
            if self.market_digest:
                mm = self.market_digest.get_metrics()
                lines.append(f"💰 <b>CoinGecko:</b>")
                lines.append(f"   Запросов/мин: {mm['coingecko_calls_last_minute']}/30")
                lines.append(f"   Кэш: {mm['cache_entries']} записей")
            await message.answer("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            await message.answer("❌ Ошибка получения метрик")

    @handle_telegram_errors
    async def handle_button(self, message: types.Message):
        """Обработка нажатий на reply-кнопки"""
        user_id = message.from_user.id
        text = message.text

        # Глобальные кнопки — не требуют чтения state из БД
        if text == "📊 Мой дайджест":
            await self._send_digest_now(message)
            return

        if text == "📰 Новости":
            await self._safe_set_state(user_id, "news")
            await message.answer(
                "📰 <b>Новости</b>\n\nВыберите категорию:",
                parse_mode="HTML",
                reply_markup=self.keyboards["news"]()
            )
            return

        if text == "⚙️ Настройки":
            try:
                await self._safe_set_state(user_id, "settings")
                await self.db.add_user(user_id, message.from_user.username, message.from_user.first_name)
                prefs = await self.db.get_user_preferences(user_id)
                broadcast_hour = await self.db.get_broadcast_hour(user_id)
                logger.info(f"Settings opened for {user_id}: {len(prefs)} cats, hour={broadcast_hour}")
                await message.answer(
                    "⚙️ <b>Настройки категорий</b>\n\nНажмите на категорию, чтобы включить/выключить:",
                    parse_mode="HTML",
                    reply_markup=self.keyboards["settings"](prefs, broadcast_hour)
                )
            except Exception as e:
                logger.error(f"Error opening settings for {user_id}: {e}", exc_info=True)
                await message.answer(
                    "❌ Ошибка открытия настроек. Попробуйте позже.",
                    reply_markup=self.keyboards["main"]()
                )
            return

        if text == "🌍 Сменить город":
            await self._safe_set_state(user_id, "city")
            await message.answer(
                "🌍 <b>Выберите город:</b>",
                parse_mode="HTML",
                reply_markup=self.keyboards["city"]()
            )
            return

        if text == "🔙 Назад в меню":
            await self._safe_set_state(user_id, "main")
            await message.answer(
                "🏠 <b>Главное меню</b>",
                parse_mode="HTML",
                reply_markup=self.keyboards["main"]()
            )
            return

        # Для state-зависимых кнопок читаем state из БД один раз
        state = await self.db.get_user_state(user_id)

        if state == "settings":
            await self._handle_settings_button(message)
        elif state == "time":
            await self._handle_time_button(message)
        elif state == "city":
            await self._handle_city_button(message)
        elif state == "news":
            await self._handle_news_button(message)
        elif state == "crypto":
            await self._handle_crypto_button(message)
        else:
            await self._safe_set_state(user_id, "main")
            await message.answer(
                "🤔 Не понимаю. Используйте кнопки меню.",
                reply_markup=self.keyboards["main"]()
            )

    async def _safe_set_state(self, user_id: int, state: str) -> None:
        """Обёртка над set_user_state с fallback на 'main' при ValueError."""
        try:
            await self.db.set_user_state(user_id, state)
        except ValueError as e:
            logger.error(f"Invalid state '{state}' for user {user_id}: {e}")
            await self.db.set_user_state(user_id, "main")

    # === SETTINGS HANDLERS ===
    async def _handle_settings_button(self, message: types.Message):
        user_id = message.from_user.id
        text = message.text

        if text == "🔙 Назад в меню":
            await self._safe_set_state(user_id, "main")
            await message.answer("🏠 Главное меню", reply_markup=self.keyboards["main"]())
            return

        if text.startswith("⏰ Время:"):
            await self._safe_set_state(user_id, "time")
            current_hour = await self.db.get_broadcast_hour(user_id)
            await message.answer(
                "⏰ Выберите время ежедневной рассылки:",
                reply_markup=self.keyboards["time"](current_hour)
            )
            return

        if text.startswith("───"):
            return

        clean_text = text
        if text.startswith("✅ "):
            clean_text = text[2:].strip()
        elif text.startswith("❌ "):
            clean_text = text[2:].strip()

        button_to_category = {
            "🌤 Погода": "weather",
            "💰 Криптовалюты": "crypto",
            "💱 Курсы валют": "fiat",
            "📰 Главное": "news_top",
            "🌍 В мире": "news_world",
            "💻 Технологии": "news_technology",
            "💼 Бизнес": "news_business",
            "🔬 Наука": "news_science",
            "🏥 Здоровье": "news_health",
            "⚽ Спорт": "news_sports",
            "🎬 Развлечения": "news_entertainment",
            "🏛️ Политика": "news_politics",
            "📊 Все новости": "news_all",
        }

        found_cat_key = button_to_category.get(clean_text)
        if not found_cat_key:
            logger.warning(f"Category not found: '{text}' (clean: '{clean_text}')")
            # FIX: возвращаем клавиатуру настроек, чтобы кнопки не пропали
            prefs = await self.db.get_user_preferences(user_id)
            broadcast_hour = await self.db.get_broadcast_hour(user_id)
            await message.answer(
                "🤔 Неизвестная категория.",
                reply_markup=self.keyboards["settings"](prefs, broadcast_hour)
            )
            return

        prefs = await self.db.get_user_preferences(user_id)
        new_state = not prefs.get(found_cat_key, True)
        logger.info(f"Toggle: user={user_id}, cat={found_cat_key} -> {new_state}")
        await self.db.toggle_preference(user_id, found_cat_key, new_state)

        prefs = await self.db.get_user_preferences(user_id)
        broadcast_hour = await self.db.get_broadcast_hour(user_id)
        cat_name = CATEGORIES[found_cat_key]
        status_text = "включена ✅" if new_state else "выключена ❌"
        await message.answer(
            f"⚙️ {cat_name}: {status_text}",
            parse_mode="HTML",
            reply_markup=self.keyboards["settings"](prefs, broadcast_hour)
        )

    async def _handle_time_button(self, message: types.Message):
        user_id = message.from_user.id
        text = message.text

        if text == "🔙 Назад в настройки":
            await self._safe_set_state(user_id, "settings")
            prefs = await self.db.get_user_preferences(user_id)
            broadcast_hour = await self.db.get_broadcast_hour(user_id)
            await message.answer("⚙️ Настройки",
                                 reply_markup=self.keyboards["settings"](prefs, broadcast_hour))
            return

        try:
            time_str = text.replace("✓ ", "").replace("✓", "").strip()
            hour = int(time_str.split(":")[0])
            if hour in BROADCAST_HOURS:
                await self.db.set_broadcast_hour(user_id, hour)
                await self._safe_set_state(user_id, "settings")
                prefs = await self.db.get_user_preferences(user_id)
                await message.answer(
                    f"✅ Время рассылки: {hour:02d}:00 МСК",
                    reply_markup=self.keyboards["settings"](prefs, hour)
                )
            else:
                await message.answer("❌ Недопустимое время")
        except (ValueError, IndexError):
            await message.answer("❌ Ошибка разбора времени")

    async def _handle_city_button(self, message: types.Message):
        user_id = message.from_user.id
        text = message.text

        if text == "🔙 Назад в меню":
            await self._safe_set_state(user_id, "main")
            await message.answer("🏠 Главное меню", reply_markup=self.keyboards["main"]())
            return

        city_name = text.lower().strip()
        if city_name in CITY_COORDINATES:
            try:
                await self.db.update_city(user_id, city_name)
                await self._safe_set_state(user_id, "main")
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

    async def _handle_news_button(self, message: types.Message):
        user_id = message.from_user.id
        text = message.text

        if text == "🔙 Назад в меню":
            await self._safe_set_state(user_id, "main")
            await message.answer("🏠 Главное меню", reply_markup=self.keyboards["main"]())
            return

        if text == "💰 Крипто-дайджест":
            await self._show_crypto_digest(message)
            return

        if not self.news_digest:
            await message.answer("❌ Сервис новостей недоступен")
            return

        button_to_category = {
            "📰 Главное": "top",
            "🌍 В мире": "world",
            "💻 Технологии": "technology",
            "💼 Бизнес": "business",
            "🔬 Наука": "science",
            "🏥 Здоровье": "health",
            "⚽ Спорт": "sports",
            "🎬 Развлечения": "entertainment",
            "🏛️ Политика": "politics",
        }

        if text == "📊 Все новости":
            digest_text = self.news_digest.get_combined_digest(max_per_category=3)
            await message.answer(digest_text, parse_mode="HTML", disable_web_page_preview=True)
            return

        category = button_to_category.get(text)
        if not category:
            await message.answer("❌ Неизвестная категория")
            return

        digest_text = self.news_digest.get_news_digest(language="ru", category=category, max_items=5)
        await message.answer(digest_text, parse_mode="HTML", disable_web_page_preview=True)

    async def _show_crypto_digest(self, message: types.Message):
        user_id = message.from_user.id
        if not self.market_digest:
            await message.answer("❌ Сервис недоступен")
            return
        crypto_unlocked = await self.db.is_crypto_unlocked(user_id)
        if not crypto_unlocked:
            await self._show_crypto_locked(message)
            return
        digest_text = self.market_digest.get_digest()
        await message.answer(digest_text, parse_mode="HTML", disable_web_page_preview=True)

    async def _show_crypto_locked(self, message: types.Message):
        user_id = message.from_user.id
        bot_username = (await self.bot.get_me()).username
        ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        text = (
            "🔒 <b>Крипто-дайджест заблокирован</b>\n\n"
            "Пригласи <b>одного друга</b> и разблокируй эту функцию!\n\n"
            f"🔗 Твоя реферальная ссылка:\n<code>{ref_link}</code>\n\n"
            "⏳ Ссылка действительна 7 дней"
        )
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="📋 Скопировать ссылку", url=ref_link)]]
        )
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

    async def _handle_crypto_button(self, message: types.Message):
        user_id = message.from_user.id
        text = message.text

        if text == "🔙 Назад в меню":
            await self._safe_set_state(user_id, "main")
            await message.answer("🏠 Главное меню", reply_markup=self.keyboards["main"]())
            return

        if not self.market_digest:
            await message.answer("❌ Сервис недоступен")
            return

        if text == "🔄 Обновить крипто":
            await self.market_digest.refresh_all()
            digest_text = self.market_digest.get_digest()
            await message.answer(digest_text, parse_mode="HTML", reply_markup=self.keyboards["crypto"]())

    # === DIGEST ===
    @rate_limit(seconds=RATE_LIMIT_SECONDS)
    @track_usage("digest")
    @handle_telegram_errors
    async def _send_digest_now(self, message: types.Message):
        user_id = message.from_user.id
        try:
            user_city = await self.db.get_user_city(user_id)
            lat, lon = CITY_COORDINATES.get(user_city, (DEFAULT_LAT, DEFAULT_LON))
            data = await self.cache_manager.get_data(lat, lon)
            prefs = await self.db.get_user_preferences(user_id)
            message_text = await self._format_digest(data, prefs, user_city)
            await message.answer(message_text, parse_mode="HTML", disable_web_page_preview=True)
            logger.info(f"📬 Дайджест отправлен {user_id}")
        except Exception as e:
            logger.error(f"Ошибка дайджеста {user_id}: {e}")
            await message.answer("⚠️ Временно недоступно\n\nПопробуйте позже.")

    async def _format_digest(self, cache_data: dict, prefs: dict, city: str) -> str:
        msk = timezone(timedelta(hours=3))
        current_time = datetime.now(msk).strftime('%d.%m %H:%M')
        parts = [f"🗞 <b>ИнфоХаб</b> • {current_time} MSK • {html.escape(city.title())}"]

        if prefs.get("weather") and cache_data.get("weather"):
            w = cache_data["weather"]
            temp = w.get('temperature', 'N/A')
            condition = w.get('condition', 'Неизвестно')
            emoji = w.get('condition_emoji', '🌡️')
            precip_type = w.get('precipitation_type')
            cloud_cover = w.get('cloud_cover', 0)
            precip_str = f", {precip_type}" if precip_type else (", без осадков" if cloud_cover < 20 else "")
            parts.append(f"\n{emoji} <b>Погода:</b> {temp}°C, {condition}{precip_str}")
        elif prefs.get("weather"):
            parts.append("\n🌡️ <b>Погода:</b> временно недоступно")

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

        if prefs.get("fiat") and cache_data.get("fiat"):
            fiat = cache_data["fiat"]
            rates = fiat.get("rates", {})
            parts.append(f"\n💱 <b>Курсы к рублю</b> ({fiat.get('date', 'N/A')}):")
            for code, name in [("USD", "🇺🇸 Доллар"), ("EUR", "🇪🇺 Евро"), ("CNY", "🇨🇳 Юань")]:
                val = rates.get(code)
                parts.append(f"  {name}: {val} ₽" if val else f"  {name}: недоступно")

        if self.news_digest:
            news_parts = []
            news_category_map = {
                "news_top": ("top", "📰 Главное"),
                "news_world": ("world", "🌍 В мире"),
                "news_technology": ("technology", "💻 Технологии"),
                "news_business": ("business", "💼 Бизнес"),
                "news_science": ("science", "🔬 Наука"),
                "news_health": ("health", "🏥 Здоровье"),
                "news_sports": ("sports", "⚽ Спорт"),
                "news_entertainment": ("entertainment", "🎬 Развлечения"),
                "news_politics": ("politics", "🏛️ Политика"),
            }
            for pref_key, (api_category, label) in news_category_map.items():
                if prefs.get(pref_key):
                    articles = self.news_digest.get_cached_articles(
                        language="ru", category=api_category, max_items=2
                    )
                    if articles:
                        news_parts.append(f"\n{label}:")
                        for item in articles:
                            raw_title = item.get("title", "Без заголовка")
                            title = html.escape((raw_title[:50] + "...") if len(raw_title) > 50 else raw_title)
                            link = item.get("url", "#")
                            news_parts.append(f" • <a href='{link}'>{title}</a>")

            if prefs.get("news_all") and not news_parts:
                combined = self.news_digest.get_combined_digest(max_per_category=2)
                lines = combined.split("\n")
                if lines and "Новости дня" in lines[0]:
                    lines = lines[1:]
                news_parts = ["\n📊 " + line for line in lines if line.strip()]

            if news_parts:
                parts.append("\n📰 <b>Новости:</b>")
                parts.extend(news_parts)
            elif any(prefs.get(k) for k in NEWS_CATS_CONFIG.keys()):
                parts.append("\n📰 <b>Новости:</b> временно недоступно")

        parts.append(f"\n\n{PREMIUM_PROMO_TEXT}")
        return "\n".join(parts)

    # === BROADCAST ===
    async def hourly_broadcast(self, hour: int):
        logger.info(f"🚀 Запуск рассылки {hour:02d}:00")
        try:
            await self.cache_manager.force_refresh()
            users = await self.db.get_users_by_broadcast_hour(hour)
            logger.info(f"📬 Рассылка {len(users)} пользователям ({hour:02d}:00)")
            sent = failed = 0
            for user in users:
                if self._shutdown_requested:
                    break
                try:
                    user_city = user.get("city", "москва")
                    lat, lon = CITY_COORDINATES.get(user_city, (DEFAULT_LAT, DEFAULT_LON))
                    user_data = await self.cache_manager.get_data(lat, lon)
                    msg_text = await self._format_digest(user_data, user["preferences"], user_city)
                    await self.bot.send_message(
                        chat_id=user["user_id"], text=msg_text,
                        parse_mode="HTML", disable_web_page_preview=True
                    )
                    sent += 1
                    await asyncio.sleep(0.05)
                except TelegramBadRequest as e:
                    if "bot was blocked" in str(e).lower():
                        logger.debug(f"User {user['user_id']} blocked bot")
                    else:
                        logger.warning(f"Telegram error {user['user_id']}: {e}")
                    failed += 1
                except Exception as e:
                    logger.error(f"Ошибка {user['user_id']}: {e}")
                    failed += 1
            logger.info(f"✅ Рассылка {hour:02d}:00: {sent} доставлено, {failed} ошибок")
        except Exception as e:
            logger.error(f"Критическая ошибка рассылки: {e}", exc_info=True)

    # === SCHEDULER ===
    def _setup_scheduler(self):
        msk_tz = timezone(timedelta(hours=3), name="MSK")
        self.scheduler = AsyncIOScheduler(timezone=msk_tz)
        for hour in BROADCAST_HOURS:
            self.scheduler.add_job(
                self.hourly_broadcast, trigger="cron", hour=hour, minute=0,
                id=f"daily_digest_{hour}", misfire_grace_time=3600, kwargs={"hour": hour}
            )
        self.scheduler.add_job(
            lambda: asyncio.create_task(self.cache_manager.force_refresh()),
            trigger="interval", minutes=30, id="cache_refresh", misfire_grace_time=300
        )
        if self.market_digest:
            self.scheduler.add_job(
                lambda: asyncio.create_task(self.market_digest.refresh_all()),
                trigger="interval", minutes=5, id="market_digest_refresh", misfire_grace_time=120
            )
        if self.news_digest:
            self.scheduler.add_job(
                lambda: asyncio.create_task(self.news_digest.refresh_all()),
                trigger="interval", hours=1, id="news_digest_refresh", misfire_grace_time=600
            )
        self.scheduler.start()
        logger.info(f"📅 Планировщик запущен ({BROADCAST_HOURS})")

    # === LIFECYCLE ===
    async def on_startup(self):
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК БОТА INFOHUB")
        logger.info("=" * 60)

        if not BOT_TOKEN or ":" not in BOT_TOKEN:
            raise ValueError("BOT_TOKEN is missing or invalid")

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
        logger.info("✅ Крипто-дайджест готов")

        self.news_digest = NewsDigest(NEWS_CACHE_PATH)
        await self.news_digest.refresh_all()
        logger.info("✅ NewsDigest инициализирован")

        self.keyboards = self._create_keyboards()
        self._setup_scheduler()

        me = await self.bot.get_me()
        logger.info(f"✅ Бот: @{me.username} (ID: {me.id})")
        logger.info(f"📊 Пользователей: {await self.db.get_user_count()}")
        logger.info("=" * 60)
        logger.info("🎯 БОТ ГОТОВ К РАБОТЕ")
        logger.info("=" * 60)

    async def on_shutdown(self):
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
        if self.db:
            await self.db.close()
        if self.bot and self.bot.session and not self.bot.session.closed:
            await self.bot.session.close()
        logger.info("✅ Бот остановлен")

    def register_handlers(self):
        self.dp.message(CommandStart())(self.cmd_start)
        self.dp.message(Command("help"))(self.cmd_help)
        self.dp.message(Command("ping"))(self.cmd_ping)
        self.dp.message(Command("stats"))(self.cmd_stats)
        self.dp.message(Command("api"))(self.cmd_api_metrics)
        self.dp.message(F.text)(self.handle_button)

    async def run(self):
        self.bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
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
            logger.info("⚠️ Ctrl+C")
        except Exception as e:
            logger.critical(f"🛑 КРИТИЧЕСКАЯ ОШИБКА: {e}")
            logger.critical(traceback.format_exc())
            raise
        finally:
            if self.bot and self.bot.session and not self.bot.session.closed:
                await self.bot.session.close()
            logger.info("🏁 Скрипт завершён")


def check_critical_files():
    logger.info("🔍 Проверка файловой системы...")
    try:
        test_file = Path(__file__).parent / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
        logger.info("✅ Права на запись: OK")
    except (IOError, OSError, PermissionError) as e:
        logger.warning(f"⚠️ Права ограничены: {e}")
    logger.info(f"{'✅' if DB_PATH.exists() else '📝'} DB: {DB_PATH}")
    logger.info(f"{'✅' if CACHE_PATH.exists() else '📝'} Cache: {CACHE_PATH}")
    logger.info(f"{'✅' if MARKET_CACHE_PATH.exists() else '📝'} Market: {MARKET_CACHE_PATH}")
    logger.info(f"{'✅' if NEWS_CACHE_PATH.exists() else '📝'} News: {NEWS_CACHE_PATH}")


def main():
    logger.info("📍 Точка входа")
    try:
        check_critical_files()
        app = BotApp()
        asyncio.run(app.run())
    except SystemExit as e:
        sys.exit(e.code)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        logger.critical(f"🛑 {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()