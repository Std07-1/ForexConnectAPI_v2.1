## Новий канал стану конектора: `fxcm:status`

Окрім даних (`fxcm:ohlcv`) та живої ціни (`fxcm:price_tik`), FXCM-конектор тепер публікує **агрегований статус** у окремий канал:

* **за замовчуванням:** `fxcm:status`
* **можна змінити:**
  через ENV `FXCM_STATUS_CHANNEL` або `stream.status_channel` у `runtime_settings.json`.

Цей канал зроблений спеціально для зовнішніх систем/таблиць, щоб бачити:

* чи конектор **стрімить / дрімає / спить / впав**;
* чи ринок **відкритий / закритий**;
* чи **живі** канали ціни та OHLCV;
* в якій **сесії** зараз ринок та коли наступне відкриття.

### Важливо: все решта (канали fxcm:heartbeat, внутрішні поля backoff/history/tick_cadence тощо) — внутрішня діагностика AiOne/SMC. Споживачам не потрібно їх читати або інтерпретувати — усі ці сигнали вже зведені в простий статус fxcm:status

---

## Формат повідомлення `fxcm:status`

Повідомлення — один JSON-об’єкт, який оновлюється разом із heartbeat/market_status (як правило, раз на кілька секунд):

```json
{
  "ts": 1764867000.0,

  "process": "stream",        // stream | idle | sleep | error
  "market": "open",           // open | closed | unknown

  "price": "ok",              // ok | stale | down
  "ohlcv": "ok",              // ok | delayed | down

  "session": {
    "name": "Tokyo",          // Назва сесії (Tokyo / London / New York / ...)
    "tag": "TOKYO",           // Машинний тег (як у FXCM sessions)
    "state": "open",          // open | closed | preopen

    "current_open_utc": "2025-12-05T00:00:00Z",
    "current_close_utc": "2025-12-05T09:00:00Z",

    "next_open_utc": "2025-12-05T09:00:00Z",
    "seconds_to_close": 5400,        // скільки залишилось до кінця поточної
    "seconds_to_next_open": 5400     // скільки до наступної сесії
  },

  "note": "ok"                // Короткий текстовий підсумок для людини
}
```

### Як це читати в таблиці

Рекомендується мапити так:

* `process`

  * `stream` → конектор стрімить, усе працює в реальному часі;
  * `idle` → ринок тихий, cadence сповільнений (економ-режим, **не помилка**);
  * `sleep` → авто-сон поза основними сесіями (за нашими правилами, **не помилка**);
  * `error` → некоректний стан (потребує уваги).

* `market`

  * `open` → ринок відкритий;
  * `closed` → ринок закритий (вечір/вихідні/свята);
  * `unknown` → короткий перехідний стан, можна трактувати як «нема впевненості».

* `price` (стан каналу `fxcm:price_tik`)

  * `ok` → тикові ціни свіжі, «живий» потік;
  * `stale` → давно не було тика, краще не торгувати, поки не оновиться;
  * `down` → канал ціни неактивний.

* `ohlcv` (стан каналу `fxcm:ohlcv`)

  * `ok` → OHLCV-бари оновлюються вчасно;
  * `delayed` → бари з помітним лагом;
  * `down` → нові бари не приходять (фід для історії недоступний).

* `session`

  * `name/state` → яка сесія зараз і чи вона відкрита;
  * `state_detail` → тип закриття (поле зʼявляється лише коли `state=closed`): `intrabreak` (коротка пауза в межах дня), `overnight` (нічна перерва), `weekend` (довгий геп до неділі/свята);
  * `seconds_to_close` → скільки часу до закриття поточної;
  * `seconds_to_next_open` → скільки до наступної сесії;
  * ці поля зручно показувати окремою колонкою типу:
    `Tokyo (weekend, ~48h)`.

* `note`

  * короткий опис: `"ok"`,
    `"weekend (2d)"`,
    `"overnight pause"`,
    `"idle: quiet market, cadence x2.5"`,
    `"fxcm backoff 30s, retry later"` тощо.

---

## Як підключитись

Приклад найпростішого споживача на Python (Redis):

```python
import json
import redis

STATUS_CHANNEL = "fxcm:status"  # або свій, якщо переозначено в конфігу

r = redis.Redis(host="YOUR_REDIS_HOST", port=6379, decode_responses=True)
pubsub = r.pubsub()
pubsub.subscribe(STATUS_CHANNEL)

print(f"Listening {STATUS_CHANNEL}...")
for message in pubsub.listen():
    if message["type"] != "message":
        continue
    status = json.loads(message["data"])

    process = status["process"]
    market = status["market"]
    price_state = status["price"]
    ohlcv_state = status["ohlcv"]
    session = status.get("session", {})
    note = status.get("note", "")

    session_name = session.get("name")
    sec_to_close = session.get("seconds_to_close")

    print(
        f"FXCM process={process}, market={market}, "
        f"price={price_state}, ohlcv={ohlcv_state}, "
        f"session={session_name}, sec_to_close={sec_to_close}, note={note}"
    )
```

На основі цього ж об’єкта можна оновлювати один рядок у вашій таблиці, наприклад:

`FXCM | STREAM | OPEN | price=OK | ohlcv=OK | Tokyo (до закриття ~01:30) | ok`

---

## Підсумок для споживача

* Для **даних**:

  * бари: `fxcm:ohlcv`,
  * жива ціна: `fxcm:price_tik`.
* Для **стану конектора й сесій**:

  * статус: `fxcm:status` (простий JSON із `process/market/price/ohlcv/session/note`).

* Не потрібно:

  * читати чи розбирати fxcm:heartbeat;

  * аналізувати backoff/history_quota/tick_cadence.
  * Усі ці внутрішні сигнали вже агреговані в прості поля process/market/price/ohlcv/session/note каналу fxcm:status.

* Усі складні внутрішні метрики (heartbeat, backoff, quota тощо) ми вже агрегували в ці кілька полів, щоб вам не доводилося їх інтерпретувати самостійно.
