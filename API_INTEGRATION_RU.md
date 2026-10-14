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

## 6.3 Выставить товар через внешний endpoint

- **Метод**: `POST` (также поддерживается `PUT` для совместимости)
- **URL**: `/api/products/upload/`
- **Content-Type**: `application/json`

Поля:
- `ean` *(string, required)*: EAN товара (валидный штрихкод с контрольной суммой EAN-13/GS1).
- `controller` *(string, required)*: `jv` или `xl`.
- `title` *(string, required)*: название товара.
- `description` *(string, required)*: описание товара.
- `picture` *(array[string URL], required)*: список URL картинок. Можно передать alias `picture_urls`.
- `price` *(decimal, required)*: базовая цена. Для офферов backend сам конвертирует ее в формат Kaufland для каждого storefront.
- `size` *(string, required)*: размер.
- `color` *(string, required)*: цвет.
- `material` *(string, required)*: материал **строго на немецком языке** (например: `Holz`, `Sperrholz`, `Textil`, `Leder`, `Metall`, `Kunststoff`). Кириллица запрещена и вызывает ошибку 400.
- `material_composition` *(string, optional)*: процентный состав текстиля/обивки (например: `100% Polyester`, `100% Leder`). Обязателен для мягкой мебели/стульев/диванов по регламенту ЕС. Не передавайте фиктивные строки вроде `"No information required"`.
- `delivery` *(integer, required)*: handling time.
- `height` *(decimal, required)*: высота.
- `length` *(decimal, required)*: длина.
- `width` *(decimal, required)*: ширина.
- `amount` *(integer, optional, default `20`)*: количество.
- `id_offer` *(string, optional, default `ean`)*: ID оффера.
- `storefronts` *(array[string], optional)*: storefronts для офферов. Значения: `de`, `cz`, `sk`, `pl`, `at`, `fr`, `it`. Если не передано, используются все.

Пример:

```json
{
  "ean": "4071489790597",
  "controller": "jv",
  "title": "Bürosessel Beige 70x70x100 cm",
  "description": "Exquisiter Bürosessel in edlem Beige mit Metallgestell.",
  "picture": ["https://kl.automatonsoft.de/upload-images/example.jpg"],
  "price": "899.00",
  "size": "70x70x100 cm",
  "color": "Beige",
  "material": "Holz",
  "material_composition": "100% Polyester",
  "delivery": 28,
  "height": "100",
  "length": "70",
  "width": "70",
  "amount": 20,
  "storefronts": ["de", "at"]
}
```

Ответы:
- `201 Created`: товар и офферы успешно созданы/обновлены в Kaufland.
- `400 Bad Request`: ошибка валидации входных данных до отправки в Kaufland (неверный EAN, кириллица в `material`, фиктивный `material_composition`). Возвращает структурированный объект `errors` с пояснением каждого поля.
- `422 Unprocessable Entity`: product-data или unit отклонен Kaufland (с указанием точной причины в поле `reason`).
- `502 Bad Gateway`: ошибка upstream Kaufland на этапе связи с маркетплейсом.

Пример ответа 400 при ошибке:
```json
{
  "success": false,
  "message": "Product validation failed before Kaufland submission",
  "errors": {
    "material": [
      "Material must be specified in German. Received Cyrillic value: 'фанера'. Allowed examples: 'Holz', 'Sperrholz', 'Textil', 'Leder', 'Metall', 'Kunststoff'."
    ],
    "material_composition": [
      "Invalid value. Kaufland rejects placeholder values like 'No information required'. Provide a valid fiber composition (e.g. '100% Polyester', '100% Leder') or omit the field."
    ]
  },
  "ean": "4071489790597"
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

def check_product_status(ean: str, controller: str = "jv", storefront: str = "de") -> dict:
    """
    Проверка реального статуса публикации и валидации одиночного товара на Kaufland.
    Возвращает статус (LIVE / BLOCKED / PENDING / NOT_FOUND), флаг is_live,
    атрибуты карточки и список обнаруженных ошибок/причин блокировки (issues_detected).
    """
    r = requests.get(
        f"{BASE}/api/products/status/{ean}/",
        params={"controller": controller, "storefront": storefront},
        timeout=30,
    )
    return r.json()
```

---

## 13) Проверка статуса и валидации товара на Kaufland (Одиночная проверка)

Эндпоинт предназначен для сторонних проектов, создающих товары через `KAUFLAND_API_CREATE_ENDPOINT=/api/products/upload/` и `KAUFLAND_API_UPDATE_ENDPOINT=/api/products/ean/change/`, чтобы проверить реальное состояние товара без ручного входа в Seller Portal.

- **Метод**: `GET`
- **URL**: 
  - `/api/products/status/{ean}/?controller=jv`
  - или `/api/products/status/?ean={ean}&controller=jv`
- **Авторизация**: публичный (AllowAny)
- **Параметры (Query)**:
  - `controller` (обязательный): `jv` или `xl`
  - `storefront` (опционально): `de` (по умолчанию), `pl`, `cz`, `sk`, `at` или `all`

### Возможные статусы (`status`):
- `LIVE`: Оффер активен на витрине (`is_live: true`), доступен покупателям.
- `BLOCKED`: Оффер заблокирован (`is_live: false`), найдены ошибки валидации карточки (`is_valid: false`, пустые картинки, кириллица, некорректный состав и т.д.).
- `PENDING`: Оффер отправлен (`is_live: false`), но явных ошибок атрибутов нет (идет обработка картинок или модерация каталога Kaufland).
- `NOT_FOUND` (`HTTP 404`): Товар с данным EAN не существует в Kaufland (нет ни офферов, ни карточки).

---

### Пример 1: Товар активен (`LIVE`, HTTP 200)

```json
{
  "success": true,
  "ean": "4069943360264",
  "controller": "jv",
  "status": "LIVE",
  "is_live": true,
  "is_valid": true,
  "summary": "Product is LIVE and actively listed for buyers on Kaufland.",
  "product_url": "https://www.kaufland.de/product/551758076/",
  "storefronts": {
    "de": {
      "is_live": true,
      "status": "AVAILABLE",
      "id_unit": 392909971437,
      "id_offer": "4069943360264",
      "id_product": 551758076,
      "price": 2249.0,
      "amount": 20,
      "handling_time": 30,
      "date_inserted": "2026-09-03T15:06:17+02:00",
      "date_lastchange": "2026-09-07T10:29:53+02:00"
    }
  },
  "attributes": {
    "title": "Hochbett aus Holz – Ideal für platzsparendes Wohnen und Stauraum.",
    "manufacturer": "AEA GmbH & Co. KG",
    "main_picture": "https://media.cdn.kaufland.de/product-images/original/e3814f292e5cf57400c686adab2b2299.jpg",
    "category_id": 2821,
    "category": "Laufgitter",
    "pictures_count": 2,
    "material": "Textil",
    "material_composition": "100% Polyester"
  },
  "issues_detected": []
}
```

---

### Пример 2: Товар заблокирован с ошибками (`BLOCKED`, HTTP 200)

```json
{
  "success": true,
  "ean": "4071489790597",
  "controller": "jv",
  "status": "BLOCKED",
  "is_live": false,
  "is_valid": false,
  "summary": "Product is NOT LIVE (is_live: false) due to detected attribute validation issues.",
  "product_url": "https://www.kaufland.de/product/577536782/",
  "storefronts": {
    "de": {
      "is_live": false,
      "status": "AVAILABLE",
      "id_unit": 392964746611,
      "id_offer": "4071489790597",
      "id_product": 577536782,
      "price": 899.0,
      "amount": 20,
      "handling_time": 28,
      "date_inserted": "2026-09-09T12:45:45+02:00",
      "date_lastchange": "2026-09-09T13:26:45+02:00"
    }
  },
  "attributes": {
    "title": "Bürosessel Beige",
    "manufacturer": "AEA",
    "category_id": 5831
  },
  "issues_detected": [
    "Kaufland product catalog flag 'is_valid' is FALSE (product failed internal validation or requires attribute approval).",
    "Product has no main image (main_picture is empty on Kaufland).",
    "Product-data attributes (material, description, composition) not active in Kaufland catalog. Offer is held in draft/need-attributes state."
  ]
}
```

---

### Пример 3: Товар не найден (`NOT_FOUND`, HTTP 404)

```json
{
  "success": false,
  "ean": "1111111111111",
  "controller": "jv",
  "status": "NOT_FOUND",
  "is_live": false,
  "summary": "Product with EAN 1111111111111 was not found on Kaufland.",
  "storefronts": {},
  "attributes": {},
  "issues_detected": [
    "Product does not exist in Kaufland catalog (neither product-data, product, nor units found)."
  ]
}
```

