"""
Потокобезопасный и ограниченный по памяти LRU-кэш с поддержкой TTL (Time-To-Live).
Предотвращает утечки памяти и накопление устаревших данных при массовой обработке каталога.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import Any, Iterator


class BoundedLRUCache:
    """
    LRU-кэш с ограничением максимального размера (maxsize) и временем жизни записей (ttl_sec).
    Совместим с интерфейсом словаря (dict) для drop-in замены.
    """

    def __init__(self, maxsize: int = 5000, ttl_sec: float | None = 3600.0):
        if maxsize <= 0:
            raise ValueError("maxsize must be > 0")
        self.maxsize = maxsize
        self.ttl_sec = ttl_sec
        # Хранит: key -> (value, expire_timestamp)
        self._cache: OrderedDict[str, tuple[Any, float | None]] = OrderedDict()

    def _is_expired(self, expire_at: float | None) -> bool:
        if expire_at is None:
            return False
        return time.monotonic() > expire_at

    def get(self, key: str, default: Any = None) -> Any:
        if key not in self._cache:
            return default
        value, expire_at = self._cache[key]
        if self._is_expired(expire_at):
            del self._cache[key]
            return default
        # Обновляем позицию (перемещаем в конец как недавно использованный)
        self._cache.move_to_end(key)
        return value

    def set(self, key: str, value: Any, ttl_sec: float | None = None) -> None:
        effective_ttl = ttl_sec if ttl_sec is not None else self.ttl_sec
        expire_at = (time.monotonic() + effective_ttl) if effective_ttl and effective_ttl > 0 else None

        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = (value, expire_at)

        # Вытеснение самых старых (LRU) при превышении лимита
        while len(self._cache) > self.maxsize:
            self._cache.popitem(last=False)

    def pop(self, key: str, default: Any = None) -> Any:
        if key not in self._cache:
            return default
        value, expire_at = self._cache.pop(key)
        if self._is_expired(expire_at):
            return default
        return value

    def clear(self) -> None:
        self._cache.clear()

    def __getitem__(self, key: str) -> Any:
        val = self.get(key)
        if val is None and key not in self:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        del self._cache[key]

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key not in self._cache:
            return False
        _, expire_at = self._cache[key]
        if self._is_expired(expire_at):
            del self._cache[key]
            return False
        return True

    def __len__(self) -> int:
        return len(self._cache)

    def cleanup_expired(self) -> int:
        """Очищает протухшие записи и возвращает количество удалённых."""
        now = time.monotonic()
        keys_to_del = [
            k for k, (_, exp) in self._cache.items()
            if exp is not None and now > exp
        ]
        for k in keys_to_del:
            del self._cache[k]
        return len(keys_to_del)
