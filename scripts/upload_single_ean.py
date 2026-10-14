"""
Загрузка ОДНОГО EAN на Kaufland.

Запуск:  python upload_single_ean.py
Скрипт спросит EAN, сам определит аккаунт (JV / XL) по каталогу Aftercool
и загрузит товар через существующую логику основного скрипта.

Положи файл в ту же папку, где лежит основной скрипт.
"""
import asyncio
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import reupload_aftercool_catalog as r

# ---------------------------------------------------------------------------
# Настройки (при необходимости меняются тут)
# ---------------------------------------------------------------------------
DRY_RUN = False                 # True = только проверка, без отправки в Kaufland
ALLOW_ALL_FACTORIES = False     # True = игнорировать allowed_factories.txt
ALLOW_CAT_FALLBACK = False      # True = разрешить fallback-категорию «Möbel»
STOREFRONTS = r.DEFAULT_STOREFRONTS
RATE_LIMIT = 19.0


def _isolate_failed_items_file():
    """
    Одиночный запуск не должен трогать общую очередь .failed_items_{account}.json:
    иначе финальный retry-проход переобработает ВСЕ старые упавшие товары,
    а запись перезатрёт файл. Поэтому для одиночных запусков — отдельный файл.
    """
    orig_init = r.FailedItemsManager.__init__

    def patched_init(self, account: str):
        orig_init(self, account)
        self.file_path = r.BACKEND_DIR / f".failed_items_single_{account}.json"
        self.file_path.unlink(missing_ok=True)

    r.FailedItemsManager.__init__ = patched_init


async def main():
    if len(sys.argv) > 1:
        raw = sys.argv[1].strip()
    else:
        raw = input("Напишите ean: ").strip()
    ean = r.normalize_ean(raw)
    if not ean or not r.is_valid_ean(ean):
        print(f"Некорректный EAN: '{raw}' (после нормализации: '{ean}')")
        return

    logs_dir = r.BACKEND_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"reupload_single_{ean}_{ts}.log"
    logger = r.setup_logger(log_file)

    # 1. Определяем аккаунт по каталогу Aftercool
    reconciler = r.CatalogReconciler(service=r.AftercoolService())
    try:
        cookie = await reconciler.service.login()
        logger.info("Pre-indexing Aftercool catalogs (JV + XL)...")
        await reconciler.preload_indices(cookie, logger=logger)
    except Exception as e:
        logger.error(f"Не удалось загрузить индекс Aftercool: {e}")
        return

    if not reconciler.is_initialized:
        logger.error("CatalogReconciler не инициализирован. Остановка.")
        return

    account, ownership_status = reconciler.resolve_target_account(ean)
    if account is None:
        print(f"EAN {ean} не найден ни в JV, ни в XL (Aftercool). Загружать нечего.")
        return

    print(f"EAN {ean} принадлежит аккаунту {account.upper()} (status={ownership_status}). Запускаю...")

    # 2. Защита общих файлов состояния основного прогона
    # process_account в конце удаляет/перезаписывает .reupload_state_{account}.json
    # (оффсет большого прогона) — сохраняем и восстанавливаем как было.
    state_file = r.BACKEND_DIR / f".reupload_state_{account}.json"
    state_backup = state_file.read_bytes() if state_file.exists() else None

    _isolate_failed_items_file()

    try:
        await r.process_account(
            account=account,
            limit=0,
            storefronts=STOREFRONTS,
            concurrency=1,
            dry_run=DRY_RUN,
            allow_cat_fallback=ALLOW_CAT_FALLBACK,
            custom_checkpoint=None,
            custom_errors=None,
            logger=logger,
            eans_whitelist={ean},
            allow_all_factories=ALLOW_ALL_FACTORIES,
            multi_account=False,
            rate_limit=RATE_LIMIT,
            reconciler=reconciler,
            skip_preindex=False,
        )
    finally:
        if state_backup is not None:
            state_file.write_bytes(state_backup)
        elif state_file.exists():
            state_file.unlink()

    print(f"\nГотово. Итог смотри в блоке FINAL SUMMARY выше. Лог: {log_file}")


if __name__ == "__main__":
    asyncio.run(main())