# middlewares.py
"""
Middleware для ограничения частоты запросов (Rate Limiting)
"""
import logging
import time
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import Message

logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, rate_limit: float = 2.0):
        super().__init__()
        self.rate_limit = rate_limit
        self.user_times: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id
        current_time = time.time()

        last_call = self.user_times.get(user_id, 0)
        if current_time - last_call < self.rate_limit:
            remaining = int(self.rate_limit - (current_time - last_call))
            logger.debug(f"Rate limit hit for user {user_id}. Wait {remaining}s")
            return None

        self.user_times[user_id] = current_time
        return await handler(event, data)