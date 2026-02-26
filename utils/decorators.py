"""
Декораторы, миддлвары и утилиты для бота
"""
import asyncio
import logging
import time
from collections import defaultdict
from functools import wraps
from typing import Any, Callable

from aiogram import BaseMiddleware
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message, TelegramObject

logger = logging.getLogger(__name__)


# === RATE LIMITING MIDDLEWARE ===

class RateLimitMiddleware(BaseMiddleware):
    """
    Миддлвар для ограничения частоты запросов от пользователя.
    Реализован на aiogram BaseMiddleware — работает для всех сообщений.
    """

    def __init__(self, rate_limit_seconds: int = 2) -> None:
        """
        Args:
            rate_limit_seconds: Минимальный интервал между запросами в секундах
        """
        super().__init__()
        self.rate_limit_seconds = rate_limit_seconds
        self._user_last_request: defaultdict[int, float] = defaultdict(float)

    async def __call__(
        self,
        handler: Callable,
        event: TelegramObject,
        data: dict,
    ) -> Any:
        # Извлекаем user_id из сообщения
        message = data.get("event_update", None)
        user_id: int | None = None

        if isinstance(event, Message):
            user_id = event.from_user.id if event.from_user else None
        elif hasattr(event, "from_user") and event.from_user:
            user_id = event.from_user.id

        if user_id is None:
            return await handler(event, data)

        now = time.monotonic()
        last = self._user_last_request[user_id]
        elapsed = now - last

        if elapsed < self.rate_limit_seconds:
            wait_time = int(self.rate_limit_seconds - elapsed) + 1
            logger.debug(f"Rate limit hit for user {user_id}, wait {wait_time}s")
            if isinstance(event, Message):
                try:
                    await event.answer(f"⏳ Подождите {wait_time} сек.")
                except Exception:
                    pass
            return None

        self._user_last_request[user_id] = now
        return await handler(event, data)


# === LEGACY DECORATOR (kept for backward compat) ===
# Устаревший декоратор оставлен для совместимости
# Следует использовать RateLimitMiddleware вместо него
def rate_limit(seconds: int = 2) -> Callable:
    """Deprecated: Используйте RateLimitMiddleware."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(event, *args, **kwargs) -> Any:
            return await func(event, *args, **kwargs)
        return wrapper
    return decorator


# === ERROR HANDLING ===
def handle_telegram_errors(func: Callable) -> Callable:
    """
    Декоратор для обработки Telegram ошибок.
    Логирует ошибки и отправляет пользователю уведомление.
    """
    @wraps(func)
    async def wrapper(event, *args, **kwargs) -> Any:
        try:
            return await func(event, *args, **kwargs)
        except TelegramBadRequest as e:
            error_msg = str(e).lower()
            if "message is not modified" in error_msg:
                pass
            elif "message to delete not found" in error_msg:
                logger.debug(f"Message already deleted in {func.__name__}")
            elif "message to edit not found" in error_msg:
                logger.debug(f"Message not found in {func.__name__}")
            else:
                logger.error(f"TelegramBadRequest in {func.__name__}: {e}")
                await _send_error_message(event, "❌ Ошибка Telegram. Попробуйте позже.")
        except asyncio.CancelledError:
            logger.debug(f"Task cancelled in {func.__name__}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}", exc_info=True)
            await _send_error_message(event, "❌ Произошла ошибка. Попробуйте позже.")
    return wrapper


async def _send_error_message(event: Any, text: str) -> None:
    """Отправляет сообщение об ошибке пользователю"""
    try:
        if hasattr(event, 'answer'):
            if hasattr(event, 'message'):
                await event.message.answer(text)
            else:
                await event.answer(text)
        elif hasattr(event, 'reply'):
            await event.reply(text)
    except Exception as e:
        logger.debug(f"Could not send error message: {e}")


# === USAGE STATISTICS ===
usage_stats: defaultdict[str, int] = defaultdict(int)


def track_usage(action: str) -> Callable:
    """
    Декоратор для отслеживания использования команд.

    Args:
        action: Название действия для статистики
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            usage_stats[action] += 1
            logger.debug(f"Usage tracked: {action} (total: {usage_stats[action]})")
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def get_usage_stats() -> dict[str, int]:
    """Возвращает статистику использования"""
    return dict(usage_stats)


def reset_usage_stats() -> None:
    """Сбрасывает статистику использования"""
    usage_stats.clear()
