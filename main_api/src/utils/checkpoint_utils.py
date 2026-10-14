"""
Структурированная система чекпоинтов для массовых операций с каталогом (Migration Pipeline).
Хранит и отслеживает состояние по ключу: (account, EAN, storefront)
Поддерживает формат JSON Lines для потоковой записи без перезаписи всего файла.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class MigrationCheckpoint:
    """
    Менеджер структурированных чекпоинтов для массовой миграции и исправления каталога.
    """

    def __init__(self, checkpoint_path: str | Path, account: str):
        self.path = Path(checkpoint_path)
        self.account = str(account).strip().lower()
        # Хранит: (ean, storefront) -> dict с деталями
        self._entries: Dict[Tuple[str, str], dict[str, Any]] = {}
        # Быстрый индекс завершённых EAN (когда все витрины SUCCESS)
        self._completed_eans: Set[str] = set()
        self.load()

    def load(self) -> int:
        """Загружает существующий чекпоинт из JSON Lines файла."""
        if not self.path.exists():
            return 0

        loaded_count = 0
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        ean = str(data.get("ean", "")).strip()
                        sf = str(data.get("storefront", "")).strip().lower()
                        if ean and sf:
                            self._entries[(ean, sf)] = data
                            loaded_count += 1
                    except json.JSONDecodeError:
                        continue
        except Exception as exc:
            logger.error(f"Error loading checkpoint {self.path}: {exc}")

        return loaded_count

    def record(
        self,
        *,
        ean: str,
        storefront: str,
        status: str,
        category_id: int | None = None,
        category_name: str | None = None,
        error: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Записывает результат обработки отдельной витрины для EAN в память и дописывает в файл.
        """
        ean_clean = str(ean).strip()
        sf_clean = str(storefront).strip().lower()
        now_iso = datetime.now(timezone.utc).isoformat()

        entry = {
            "account": self.account,
            "ean": ean_clean,
            "storefront": sf_clean,
            "status": status.upper(),
            "category_id": category_id,
            "category_name": category_name,
            "error": error,
            "updated_at": now_iso,
        }
        if extra:
            entry["extra"] = extra

        self._entries[(ean_clean, sf_clean)] = entry

        # Потоковая дозапись в файл чекпоинта
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as exc:
            logger.error(f"Failed to append to checkpoint {self.path}: {exc}")

        return entry

    def is_storefront_completed(self, ean: str, storefront: str) -> bool:
        """Проверяет, успешно ли выполнена конкретная витрина для EAN."""
        key = (str(ean).strip(), str(storefront).strip().lower())
        entry = self._entries.get(key)
        return bool(entry and entry.get("status") == "SUCCESS")

    def is_ean_fully_completed(self, ean: str, required_storefronts: list[str]) -> bool:
        """Проверяет, успешно ли выполнены ВСЕ требуемые витрины для EAN."""
        clean_ean = str(ean).strip()
        for sf in required_storefronts:
            if not self.is_storefront_completed(clean_ean, sf):
                return False
        return True

    def get_summary(self) -> dict[str, int]:
        """Возвращает общую статистику по записям чекпоинта."""
        summary = {"SUCCESS": 0, "FAILED": 0, "PARTIAL": 0, "SKIPPED": 0, "TOTAL": len(self._entries)}
        for entry in self._entries.values():
            st = entry.get("status", "UNKNOWN")
            summary[st] = summary.get(st, 0) + 1
        return summary

    def get_failed_items(self) -> list[dict[str, Any]]:
        """Возвращает список всех неуспешных записей для повторной обработки."""
        return [
            entry for entry in self._entries.values()
            if entry.get("status") in {"FAILED", "PARTIAL"}
        ]
