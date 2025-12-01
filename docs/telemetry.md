# FXCM Connector Telemetry

> Оновлено: 30 листопада 2025  Гілка `main`

Документ описує службові повідомлення, які конектор публікує у Redis. Основні канали: `fxcm:heartbeat` (технічний стан) і `fxcm:market_status` (календар FXCM). Матеріал потрібен командам дашбордів та `fxcm_ingestor` для переходу на розширену схему.

---

## 1. Загальні принципи

- **Формат:** JSON без зайвих пробілів (`json.dumps(..., separators=(",", ":"))`).
- **Час:** ISO-8601 у UTC (`ts`, `next_open_utc`) та Unix-мілісекунди (`*_ms`).
- **Сумісність:** усі нові поля опційні  старі споживачі можуть їх ігнорувати.

---

## 2. Канал `fxcm:heartbeat`

### 2.1 Обов'язкові поля

| Поле | Тип | Опис |
| --- | --- | --- |
| `type` | `"heartbeat"` | Фіксований тип. |
| `state` | `"warmup" \| "warmup_cache" \| "stream" \| "idle"` | Стадія життєвого циклу. |
| `ts` | `str` | ISO-8601 UTC без мікросекунд. |
| `context` | `object` | Розширений стан (див. нижче). |

### 2.2 Додаткові поля верхнього рівня

| Поле | Тип | Коли присутнє |
| --- | --- | --- |
| `last_bar_close_ms` | `int` | Після першої публікації барів. |
| `next_open_utc` | `str` | Якщо відомо наступне торгове вікно. |
| `sleep_seconds` | `float` | Коли цикл переходить у sleep (idle). |

### 2.3 Структура `context`

| Ключ | Тип | Стан | Призначення |
| --- | --- | --- | --- |
| `channel` | `str` | усі | Канал heartbeat (для діагностики). |
| `redis_state` | `str` | усі | Стан підключення (`"connected"`). |
| `lag_seconds` | `float` | якщо є `last_bar_close_ms` | Різниця між `now` та останнім close. |
| `next_open_ms` | `int` | якщо є `next_open_utc` | Значення `next_open_utc` у мс. |
| `next_open_in_seconds` | `float` | якщо є `next_open_utc` | Скільки секунд лишилося. |
| `cycle_sleep_seconds` | `float` | idle | Планова пауза між циклами. |
| `stream_targets` | `[{"symbol": str, "timeframe": str}]` | stream/idle | Нормалізовані символи без `/`. |
| `cache_enabled` | `bool` | усі | Чи активний файловий кеш. |
| `require_redis` | `bool` | усі | Значення `FXCM_REDIS_REQUIRED`. |
| `market_pause` | `bool` | idle | Чи призупинено роботу через ринок/FXCM. |
| `market_pause_reason` | `str` | idle | Напр. `"calendar"`, `"fxcm_unavailable"`. |
| `seconds_to_open` | `float` | idle | Дубль `next_open_in_seconds` для алертів. |
| `cycle_index` | `int` | усі | Лічильник ітерацій циклу. |
| `last_cycle_duration` | `float` | stream | Тривалість останнього циклу. |
| `source` | `str` | warmup / warmup_cache | `"fetch_history_sample"` або `"warmup_cache"`. |
| `symbol`, `timeframe` | `str` | warmup / warmup_cache | Контекст прогріву. |
| `bars_published` | `int` | warmup / warmup_cache | Скільки барів пішло у Redis. |

> Для внутрішніх експериментів додавайте ключі з префіксом `ext_`, щоб не ламати сторонні парсери.

### 2.4 Приклади

**Warmup (онлайн історія):**

```
{
  "type": "heartbeat",
  "state": "warmup",
  "ts": "2025-11-30T09:00:00+00:00",
  "last_bar_close_ms": 1764483600000,
  "context": {
    "channel": "fxcm:heartbeat",
    "redis_state": "connected",
    "lag_seconds": 0.5,
    "cache_enabled": true,
    "require_redis": true,
    "stream_targets": [{"symbol": "XAUUSD", "timeframe": "1m"}],
    "cycle_index": 0,
    "source": "fetch_history_sample",
    "symbol": "XAUUSD",
    "timeframe": "1m",
    "bars_published": 900
  }
}
```

**Idle (ринок закритий календарно):**

```
{
  "type": "heartbeat",
  "state": "idle",
  "ts": "2025-11-30T22:05:00+00:00",
  "last_bar_close_ms": 1764517200000,
  "next_open_utc": "2025-11-30T22:15:00+00:00",
  "sleep_seconds": 5,
  "context": {
    "channel": "fxcm:heartbeat",
    "redis_state": "connected",
    "lag_seconds": 540.0,
    "next_open_ms": 1764518100000,
    "next_open_in_seconds": 600.0,
    "cycle_sleep_seconds": 5.0,
    "cache_enabled": true,
    "require_redis": true,
    "stream_targets": [{"symbol": "XAUUSD", "timeframe": "1m"}],
    "market_pause": true,
    "market_pause_reason": "calendar",
    "seconds_to_open": 600.0,
    "cycle_index": 1287
  }
}
```

---

## 3. Канал `fxcm:market_status`

| Поле | Тип | Опис |
| --- | --- | --- |
| `type` | `"market_status"` | Фіксований тип. |
| `state` | `"open" \| "closed"` | Поточний статус. |
| `ts` | `str` | ISO-8601 UTC моменту публікації. |
| `next_open_utc` | `str` | Лише коли `state="closed"`. |
| `next_open_ms` | `int` | Unix time у мс для `next_open_utc`. |
| `next_open_in_seconds` | `float` | Скільки секунд залишилось до відкриття. |

**Приклад:**

```
{
  "type": "market_status",
  "state": "closed",
  "ts": "2025-11-30T22:05:00+00:00",
  "next_open_utc": "2025-11-30T22:15:00+00:00",
  "next_open_ms": 1764518100000,
  "next_open_in_seconds": 600.0
}
```

---

## 4. Рекомендації для споживачів

### 4.1 Дашборди / Prometheus bridge

1. Знімати `context.lag_seconds`, `market_pause_reason`, `last_cycle_duration` як окремі метрики.
2. `seconds_to_open` / `next_open_in_seconds` використовувати для банерів ринок відновиться через .
3. Розрізняти календарні паузи (`market_pause_reason="calendar"`) та технічні (`fxcm_unavailable`).

### 4.2 `fxcm_ingestor`

1. Warmup-повідомлення несуть `source`, `symbol`, `timeframe`, `bars_published`  логуй для аудиту.
2. Якщо `state="closed"`, використовуй `next_open_ms` як дедлайн відновлення підписок.
3. Старі інстанси інгістора можуть ігнорувати нові ключі без зміни поведінки.

### 4.3 Інцидентні боти

- `market_pause_reason` допомагає формувати повідомлення про природу простою.
- `lag_seconds > 180` сигналізує про затримку стріму.
- `last_cycle_duration` дозволяє виявляти нетипово довгі ітерації (мережеві/Redis-проблеми).

---

## 5. Контроль версій

| Версія | Зміни |
| --- | --- |
| 2.1 (2025-11-30) | Додано `context` у heartbeat та `next_open_ms` / `next_open_in_seconds` у market status. |

---

## 6. FAQ

- **Немає `context`?** Ймовірно, запущено версію конектора <2.1  онови сервіс.
- **Чи можна додавати власні ключі?** Так, але документуй і бажано використовуй префікс `ext_`.
- **Чи гарантовано, що `lag_seconds` присутній?** Ні, під час холодного старту значення може бути відсутнє.
