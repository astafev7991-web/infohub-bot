"""
Декораторы и утилиты для бота
"""
import asyncio
import logging
from functools import wraps
from collections import defaultdict
import time
from typing import Callable, Any

from aiogram.exceptions import TelegramBadRequest

logger = logging.getLogger(__name__)

# === RATE LIMITING ===
_user_last_request: defaultdict[int, float] = defaultdict(float)
RATE_LIMIT_SECONDS = 2


def rate_limit(seconds: int = RATE_LIMIT_SECONDS) -> Callable:
    """
    Декоратор для ограничения частоты запросов от пользователя.
    
    Args:
        seconds: Минимальный интервал между запросами в секундах
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(event, *args, **kwargs) -> Any:
            user_id = _get_user_id(event)
            if user_id is None:
                return await func(event, *args, **kwargs)
            
            now = time.time()
            last_request = _user_last_request[user_id]
            
            if now - last_request < seconds:
                wait_time = int(seconds - (now - last_request)) + 1
                if hasattr(event, 'answer'):
                    try:
                        await event.answer(f"⏳ Подождите {wait_time} сек.", show_alert=True)
                    except Exception:
                        pass
                logger.debug(f"Rate limit hit for user {user_id}")
                return None
            
            _user_last_request[user_id] = now
            return await func(event, *args, **kwargs)
        return wrapper
    return decorator


def _get_user_id(event: Any) -> int | None:
    """Извлекает user_id из события"""
    if hasattr(event, 'from_user') and event.from_user:
        return event.from_user.id
    if hasattr(event, 'message') and hasattr(event.message, 'from_user'):
        return event.message.from_user.id
    return None


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
                # Не логируем, это нормальная ситуация
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
