import json
from typing import Optional, Any

import redis

from app.core.config import settings


class CacheService:
    def __init__(self) -> None:
        self.redis_client = redis.from_url(settings.REDIS_URL, decode_response=True)
        self.ttl = settings.REDIS_CACHE_EXPIRE

    async def get(self, key: str) -> Optional[Any]:
        try:
            value = self.redis_client.get(key)
            if value is not None:
                return json.loads(value)
            return None
        except Exception as e:
            print(f"Cache get error: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        try:
            value_str = json.dumps(value, default=str)
            self.redis_client.setex(key, ttl or self.ttl, value_str)
            return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False

    async def delete(self, key: str) -> bool:
        try:
            self.redis_client.delete(key)
            return True
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False

    async def delete_pattern(self, pattern: str) -> int:
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            print(f"Cache delete pattern error: {e}")
            return 0

    async def clear(self) -> bool:
        try:
            self.redis_client.flushdb()
            return True
        except Exception as e:
            print(f"Cache clear error: {e}")
            return False


cache_service = CacheService()


def get_users_cache_key(
    role: Optional[str] = None,
    page: int = 1,
    limit: int = 10,
    text_search: Optional[str] = None,
) -> str:
    key = "users:list"
    if role:
        key += f":role={role}"
    if text_search:
        key += f":search={text_search}"
    key += f":page={page}:limit={limit}"
    return key


def get_user_cache_key(user_id: int) -> str:
    return f"users:id:{user_id}"


def get_users_list_pattern() -> str:
    return "users:list*"
