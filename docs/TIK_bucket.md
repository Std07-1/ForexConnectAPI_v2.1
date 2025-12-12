# План впровадження tick→bucket→1m/5m

## 1. Семантика й критичні нюанси

### 1.1 Визначення bucket

- `start_ms = floor(ts_ms / (tf_sec * 1000)) * (tf_sec * 1000)`.
- `end_ms = start_ms + tf_sec * 1000`.
- Інтервал `[start_ms, end_ms)` належить поточному bucket’у; подія з `ts_ms == end_ms` відноситься вже до наступного bucket’а.
- Докстрінги `CurrentBar` та агрегаторів повинні прямо описувати цю інтервальну семантику, щоб виключити двозначні трактування моменту закриття бару.

### 1.2 Заповнення великих gap’ів

- Поточне `fill_gaps=True` + `synthetic=True` підходить лише для коротких перерв.
- Ввести константу `MAX_SYNTHETIC_GAP_MINUTES` (15–60 хв).
- Розрахунок: `missing_minutes = (next_bucket_start_ms - curr.end_ms) / 60_000`.
- Якщо `missing_minutes > MAX_SYNTHETIC_GAP_MINUTES`, не створювати послідовність synthetic-барів:
  - закрити попередній бар як зараз;
  - примусово почати нову сесію: перший реальний тик відкриває новий live-бар;
  - опційно один synthetic-бар, щоб з’єднати close попередньої сесії з новим open, але без масового flat-заповнення.
- Так історія зберігає «дірки» лише для реальних ноторгуваних періодів (вихідні, ночі), а UI не перевантажується тисячами flat-барів.

### 1.3 Коли з’являється synthetic-бар

- Synthetic 1m формується тільки після надходження наступного тика, коли агрегатор фіксує gap.
- Це прийнятно для QA та live UI: факт «стояння» видно із запізненням, зате логіка лишається подієвою.
- Для real-time flat у майбутньому можливий таймерний фон (закриття барів за `now()`), але наразі це поза MVP.

### 1.4 Рестарт коннектора

- На рестарті зберігаємо `last_close[symbol]` із persistent-стану (Redis/файл) або останнього `complete=True` бару в UnifiedDataStore.
- Перший тик після рестарту:
  - якщо він у тому ж bucket’і, де зупинились, починаємо live-бар «з нуля»; втрачена частина хвилини не відтворюється;
  - якщо він у новому bucket’і, обробляємо як звичайний gap: закриваємо попередній бар і стартуємо новий.
- Опційно: якщо відомий `last_close` і gap ≤ 1 хв, можна створити один synthetic-бар для незакритої хвилини, але це не обов’язково на першому етапі.

### 1.5 Out-of-order тики

- Якщо `tick.ts_ms < current_bar.start_ms`, вважати тик застарілим.
- MVP-варіант: ігнорувати тик, логувати warning та збільшувати Prometheus-лічильник `fxcm_tick_agg_out_of_order_total`.
- За потреби у майбутньому можна додати невеликий буфер та сортування.

### 1.6 5m на базі 1m

- 1m — шар істини, 5m — чиста агрегація `complete=True` 1m-барів.
- 5m не генерує synthetic самостійно; якщо 1m містить synthetic, вони автоматично стають частиною 5m.
- Документація агрегатора повинна явно фіксувати цю залежність.

## 2. Контракт повідомлень `fxcm:ohlcv`

```json
{
  "symbol": "XAUUSD",
  "tf": "1m",
   "source": "tick_agg",
   "bars": [
      {
         "open_time": 1765478400000,
         "close_time": 1765478459999,
         "open": 4216.0,
         "high": 4218.5,
         "low": 4215.8,
         "close": 4217.3,
         "volume": 123.0,
         "complete": true,
         "synthetic": false,
         "source": "tick_agg",
         "tf": "1m"
      }
   ]
}
```

- Поточний конектор публікує у `fxcm:ohlcv` масив `bars` (batch). Обовʼязкові поля бару: `open_time`, `close_time`, `open`, `high`, `low`, `close`, `volume`.
- Поля `complete`, `synthetic`, `source`, `tf` — опціональні та додаються, якщо продьюсер їх має (Phase B додає їх для `tick_agg`).
- У поточній реалізації Phase B публікуються лише `complete=true` бари; live (`complete=false`) не транслюється.
- Поле `source` використовується для діагностики походження даних (наприклад, `tick_agg` vs `stream`). У стрім-режимі з `tick_aggregation_enabled=true` конектор **не змішує** FXCM history та tick-агрегацію для `1m/5m`: history-полінг для цих TF не публікується у `fxcm:ohlcv`.

## 3. Деталізований план робіт (Фази A–D)

### Фаза A — Ядро tick→OHLCV

1. **Модуль `tick_ohlcv.py`**
   - Описує dataclass-и `FxcmTick`, `OhlcvBar`, `CurrentBar`, `AggregationResult` та два агрегатори.
   - `TickOhlcvAggregator` тримає один live-бар, рахує `ticks_ingested`, `synthetic_bars_emitted`, `out_of_order_ticks` (плюс `closed_bars_emitted`), і вміє:
     - визначати bucket за формулою `[start_ms, end_ms)`;
     - закривати поточний бар під час переходу на новий bucket;
     - створювати synthetic-ланцюжок з обмеженням `max_synthetic_gap_minutes` (у хвилинах; фактична кількість барів залежить від tf);
     - повертає `AggregationResult` з масивом `closed_bars`, live-баром (complete=false) та прапором `out_of_order` для застарілих тиків.
   - `OhlcvFromLowerTfAggregator` агрегує `complete=True` 1m-бари у старші tf (наприклад, 5m), без додаткових synthetic: window form + live-бар із частково наповненим вікном, `synthetic=True` лише коли всі вхідні 1m були synthetic.
   - Докстрінги українською фіксують bucket-семантику, правила gap’ів і контракт live/complete.

2. **Юніт-тести `tests/test_tick_ohlcv.py`**
   - `test_single_minute_three_ticks_no_gap` — один bucket: open/high/low/close/volume; плюс перевірка, що тик рівно на межі `end_ms` закриває попередній бар.
   - `test_gap_shorter_than_limit_all_minutes_synthetic` — короткий gap заповнюється synthetic-баром(ами) з flat OHLC та `volume=0`.
   - `test_gap_longer_than_limit_respects_max_synthetic` — довгий gap: synthetic не створюємо, наступний реальний тик відкриває новий live-бар.
   - `test_5m_aggregator_single_bucket_five_1m_bars` — 5× `1m complete=True` → 1× `5m complete=True` з правильним open/close/high/low/volume.
   - `test_out_of_order_tick_does_not_change_bar` — тик із `ts_ms < current.start_ms` не змінює бар, але виставляє `out_of_order=True` і лічильник.

3. **QA-скрипт `tools/tick_ohlcv_qa.py`**
   - CLI аргументи: `--ticks-file`, `--format (jsonl|csv)`, `--tf-sec`, `--fill-gaps (0|1)`, `--max-synth-gap-minutes`, `--symbol`.
   - Читає JSONL/CSV з полями `symbol`, `ts_ms`, `price` або `bid+ask`, `volume`; фільтрує за символом і годує `TickOhlcvAggregator`.
   - Виводить статистику: кількість тиків, real/synthetic бари, розподіл gap’ів (у хвилинах), out-of-order тики та top-N найбільших gap’ів.
   - Приклад: `python tools/tick_ohlcv_qa.py --ticks-file data/ticks.jsonl --format jsonl --tf-sec 60 --fill-gaps 1 --max-synth-gap-minutes 60`.

### Фаза B — Інтеграція у FXCM конектор

1. **Фіче-флаги (runtime_settings.json → `stream.*`):**
   - `tick_aggregation_enabled` — вмикає tick→OHLCV агрегацію в stream-режимі (Phase B). За замовчуванням `false`.
   - `tick_aggregation_max_synth_gap_minutes` — ліміт synthetic-gap (хв) для `TickOhlcvAggregator`.
2. **Price callback:**
   - Формує `FxcmTick` з live OfferTable котирувань (ціна = mid).
   - `tick_agg_1m.ingest_tick(...)` повертає `AggregationResult`.
   - `closed_bars 1m` → публікувати в `fxcm:ohlcv`, передавати у `OhlcvFromLowerTfAggregator` для 5m.
   - У поточній реалізації Phase B публікуємо лише `complete=True` бари (live `complete=false` не шлемо).
3. **Prometheus-метрики:**
   - `fxcm_tick_agg_ticks_total`.
   - `fxcm_tick_agg_closed_bars_total{tf="60"}`.
   - `fxcm_tick_agg_synthetic_bars_total{tf="60"}`.
   - `fxcm_tick_agg_gap_minutes_histogram`.
   - `fxcm_tick_agg_out_of_order_total`.
4. **Інтеграційний тест:**
   - Мок-стрім тиків → очікувані повідомлення в `fxcm:ohlcv` із правильними полями.

Поточний стан (після Phase B):

- Конектор може публікувати додаткові поля в `fxcm:ohlcv` без ламання базового контракту:
  - top-level: `source` (наприклад, `"tick_agg"`);
  - у кожному bar (всередині `bars`): `complete`, `synthetic`, `source`, `tf`.
- HMAC-підпис (якщо увімкнений) лишається сумісним: підписуємо тільки базовий payload (`symbol`, `tf`, `bars`).

### Фаза C — UnifiedDataStore / SMC

1. **`data/fxcm_ingestor.py`:**
   - Обробник `fxcm:ohlcv` читає `complete`/`synthetic`.
   - `complete=false` — не записуємо в datastore (можна оновлювати окремий live-state для UI).
   - `complete=true` — записуємо бар незалежно від `synthetic`.
2. **SMC-Core:**
   - Живиться 5m масивами з datastore як і раніше.
   - Live 5m не подаємо в ядро.
3. **QA:**
   - Зрівняти таймстемпи XAUUSD/TradingView на проміжку.
   - Переконатися, що немає пропусків > tf_sec, окрім реальних ноторгуваних періодів.

### Фаза D — UI / Debug / Dev playground

1. **`dev_chart_playground`:**
   - Підписка на `fxcm:ohlcv`.
   - Для кожного `symbol/tf`:
     - масив complete-барів;
     - окремий live-бар (complete=false).
   - При `complete=true` додаємо в історію, live очищуємо; при `complete=false` — оновлюємо live.
2. **Debug viewer:**
   - Виводити останні N complete 1m/5m.
   - Окремо маркер LIVE для поточного бару.
   - Summary: synthetic за 60 хв, великі gap (з Prometheus).

---
Цей документ фіксує технічні вимоги та послідовність робіт для впровадження tick→bucket→1m/5m. Він є джерелом істини для розробки агрегатора, інтеграції в конектор та подальших шарів системи.
