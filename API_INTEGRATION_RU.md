# Интеграция с Kaufland API (для внешнего Django-сервиса)

Этот документ описывает, как внешнему сервису работать с вашим API.

## 1) Базовый URL и требования

- Базовый URL (prod): `https://kl.automatonsoft.de`
- Префикс API: `/api`
- Авторизация: JWT Bearer (кроме `/api/health/`)
- Обязательный параметр `controller` в операциях: `jv` или `xl`

Рекомендации:
- Использовать отдельного технического пользователя (service account).
- Хранить `access` и `refresh` токены безопасно.
- При `401` автоматически обновлять `access` через refresh.

---

## 2) Авторизация

## 2.1 Получение токена

- **Метод**: `POST`
- **URL**: `/api/token/`
- **Тело (JSON)**:

```json
{
  "username": "YOUR_USERNAME",
  "password": "YOUR_PASSWORD"
}
```

- **Успех (`200`)**:

```json
{
  "refresh": "<jwt_refresh>",
  "access": "<jwt_access>",
  "user": {
    "id": 1,
    "username": "YOUR_USERNAME"
  }
}
```

## 2.2 Обновление access-токена

- **Метод**: `POST`
- **URL**: `/api/token/refresh/`
- **Тело (JSON)**:

```json
{
  "refresh": "<jwt_refresh>"
}
```

- **Успех (`200`)**:

```json
{
  "access": "<new_jwt_access>"
}
```

Для защищенных endpoints:

```http
Authorization: Bearer <jwt_access>
```

---

## 3) Healthcheck (публичный)

- **Метод**: `GET`
- **URL**: `/api/health/`
- **Авторизация**: не требуется

### OK (`200`)

```json
{
  "status": "ok",
  "service": "kaufland-api",
  "database": "ok"
}
```

### Degraded (`503`)

```json
{
  "status": "degraded",
  "service": "kaufland-api",
  "database": "error"
}
```

---

## 4) Получить товар по EAN

- **Метод**: `GET`
- **URL**: `/api/products/ean/{ean}/?controller=jv`
- **Авторизация**: требуется
- **Path param**: `ean`
- **Query param**:
  - `controller`: `jv` или `xl` (по умолчанию `jv`)

### Успех (`200`), если найден

```json
{
  "controller": "jv",
  "ean": "1234567890123",
  "found": true,
  "storefronts": ["de", "pl"],
  "items": [ ... ],
  "message": null
}
```

### Успех (`200`), если не найден

```json
{
  "controller": "jv",
  "ean": "1234567890123",
  "found": false,
  "storefronts": [],
  "items": [],
  "message": "not found"
}
```

---

## 5) Основной endpoint операций

- **Метод**: `POST`
- **URL**: `/api/kaufland_main/`
- **Авторизация**: требуется

Поддерживаемые `mode`:
- `checker`
- `delete`
- `change_price`

## 5.1 Проверка по одному EAN (асинхронно)

- **Content-Type**: `application/json`
- **Тело**:

```json
{
  "controller": "jv",
  "mode": "checker",
  "ean": "1234567890123",
  "job_id": "optional_custom_job_id"
}
```

- **Ответ (`202`)**:

```json
{
  "message": "checker job started",
  "job_id": "<job_id>",
  "eans": ["1234567890123"]
}
```

## 5.2 Проверка по файлу (асинхронно)

- **Content-Type**: `multipart/form-data`
- **Поля**:
  - `controller`: `jv` или `xl`
  - `mode`: `checker`
  - `file`: `.csv` или `.xlsx` c колонкой `ean`
  - `job_id`: опционально

- **Ответ (`202`)**:

```json
{
  "message": "checker job started",
  "job_id": "<job_id>",
  "eans": ["..."]
}
```

## 5.3 Удаление по файлу (асинхронно)

- **Content-Type**: `multipart/form-data`
- **Поля**:
  - `controller`: `jv` или `xl`
  - `mode`: `delete`
  - `file`: `.csv`/`.xlsx` (колонка `ean`; `price` допустима, но для удаления не нужна)
  - `job_id`: опционально

- **Ответ (`202`)**:

```json
{
  "message": "delete job started",
  "job_id": "<job_id>"
}
```

## 5.4 Изменение цен по файлу (синхронно)

- **Content-Type**: `multipart/form-data`
- **Поля**:
  - `controller`: `jv` или `xl`
  - `mode`: `change_price`
  - `file`: `.csv`/`.xlsx` с колонками `ean` и `price`

- **Успех (`200`)**:
```json
"all prices updated"
```

- **Ошибка (`500`)**:
```json
"something went wrong"
```

---

## 6) Upload endpoint (JSON загрузка)

- **Метод**: `POST`
- **URL**: `/api/kaufland_main/upload_json/`
- **Авторизация**: требуется
- **Content-Type**: `multipart/form-data`

Поля:
- `controller`: `jv` или `xl`
- `mode`: `upload_product` или `upload_collection`
- `file`: `.json`
- `job_id`: опционально

## 6.1 `upload_product` (один товар)

- **Ответ (`200` или `500`)**:

```json
{
  "message": "success",
  "job_id": "<job_id>"
}
```

или

```json
{
  "message": "failed",
  "job_id": "<job_id>"
}
```

## 6.2 `upload_collection` (асинхронно)

- **Ответ (`202`)**:

```json
{
  "message": "upload job started",
  "job_id": "<job_id>"
}
```

---

## 7) Остановить задачу

- **Метод**: `POST`
- **URL**: `/api/kaufland_main/stop_job/`
- **Авторизация**: требуется
- **Тело (JSON)**:

```json
{
  "job_id": "<job_id>"
}
```

- **Успех (`200`)**:

```json
{
  "message": "stop requested",
  "job_id": "<job_id>"
}
```

---

## 8) Aftercool sync

- **Метод**: `GET`
- **URL**: `/api/aftercool_login/`
- **Авторизация**: сейчас не требуется

- **Ответ (`202`)**:

```json
{
  "message": "aftercool price sync job started",
  "job_id": "<job_id>",
  "controller": "jv",
  "ws_task": "checker",
  "progress_ws": "/ws/checker-progress/<job_id>/",
  "change_log_file": "logs/aftercool_price_changes.csv"
}
```

---

## 9) Правила `job_id`

- Можно передать свой `job_id`.
- Если не передан, backend сгенерирует сам (`uuid4().hex`) и вернет в ответе.

Рекомендация:
- Во внешнем сервисе хранить `job_id` как ключ корреляции.

---

## 10) WebSocket каналы прогресса

- Upload: `/ws/upload-progress/{job_id}/`
- Checker: `/ws/checker-progress/{job_id}/`
- Delete: `/ws/delete-progress/{job_id}/`

Типовое сообщение:

```json
{
  "job_id": "<job_id>",
  "event": "job_completed",
  "payload": { ... },
  "info": null,
  "timestamp": "2026-04-17T12:00:00.000000"
}
```

---

## 11) Рекомендуемый сценарий интеграции

1. Проверить `GET /api/health/` (ожидать `200`).
2. Получить JWT через `/api/token/`.
3. Отправить операцию (`/api/kaufland_main/` или `/api/kaufland_main/upload_json/`).
4. Забрать `job_id` из ответа.
5. Подписаться на WS-канал по `job_id`.
6. Зафиксировать итог в БД внешнего сервиса.
7. При необходимости вызвать `/api/kaufland_main/stop_job/`.

---

## 12) Пример для внешнего Django-сервиса (Python)

```python
import requests

BASE = "https://kl.automatonsoft.de"

def get_tokens(username: str, password: str) -> dict:
    r = requests.post(f"{BASE}/api/token/", json={
        "username": username,
        "password": password,
    }, timeout=30)
    r.raise_for_status()
    return r.json()

def checker_single(access: str, ean: str, controller: str = "jv") -> dict:
    r = requests.post(
        f"{BASE}/api/kaufland_main/",
        headers={"Authorization": f"Bearer {access}"},
        json={
            "controller": controller,
            "mode": "checker",
            "ean": ean,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()

def product_by_ean(access: str, ean: str, controller: str = "jv") -> dict:
    r = requests.get(
        f"{BASE}/api/products/ean/{ean}/",
        params={"controller": controller},
        headers={"Authorization": f"Bearer {access}"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()
```
