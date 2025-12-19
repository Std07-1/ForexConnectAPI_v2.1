# UPDATE

## v2.2 (2025-12-13)

- Додано повний, точний опис контрактів Redis-каналів `fxcm:*` (OHLCV / price_tik / market_status / heartbeat / status) з правилами семантики полів, required/optional ключами та нюансами HMAC.
- Розширено типізацію контрактів у схемі (TypedDict) для всіх Redis payload-ів, щоб продюсер/консьюмери мали єдине джерело правди по структурам повідомлень.
- Додано практичний самодостатній гайд для інтеграції UDS/SMC (без зовнішніх посилань): правила дедуп по `(symbol, tf, open_time)`, трактування `complete=false`, розділення `tick_ts` vs `snap_ts`, мінімальний gate по `fxcm:status`.
- Уточнено та синхронізовано документацію публічного статусу: `state_detail` існує лише для `session.state=closed` (`intrabreak`/`overnight`/`weekend`).
- Оновлено README: додано коротку “шпаргалку” для UDS/SMC по каналах `fxcm:ohlcv`, `fxcm:price_tik`, `fxcm:status`.

## v2.3 (2025-12-15)

- Додано bootstrap-режим для підготовки історії перед першим стартом SMC: утиліта `tools/cache_bootstrap.py` отримала `--min-bars` (fail-fast, exit code 2 при нестачі барів).
- Виправлено запуск `tools/cache_bootstrap.py` напряму (без `-m`): корінь репозиторію додається в `sys.path` до імпортів локальних модулів.
- Оновлено інструкцію в `docs/uds_smc_update_2025-12-13.md` для bootstrap історії (рекомендований запуск через `-m tools.cache_bootstrap`).
- Піднято дефолт `cache.warmup_bars` у `config/runtime_settings.json` до 1600 для сценарію старту SMC з достатньою історією по XAU 1m.
- Додано захист у `FXCMOfferSubscription`: якщо час з OfferTable приходить як epoch-milliseconds, він нормалізується до epoch-seconds. Це зменшує ризик стану `only_live_bars_no_complete`, коли tick-agg не може закрити бари через “майбутні” timestamp-и.
- Додано захист у `PublishDataGate` від “отруєного” cutoff у майбутньому: якщо `last_open`/`last_close` виявляються занадто попереду поточного часу, gate скидається і не блокує публікацію `complete=true` барів.
- Tick-agg: додано кламп для випадків, коли `tick_ts` приходить у майбутньому (більше ніж на 5с від `time.time()`), щоб `clock-flush` міг закривати `complete=true` бари вчасно.
- Heartbeat: додано `context.tick_agg` (queue depth, skew `tick_ts`, лічильники клампів, `last_complete_close_ms`) для швидкої діагностики `only_live_bars_no_complete`.
- Heartbeat/status: `lag_seconds` та `last_bar_close_ms` тепер синхронізуються з `PublishDataGate` (тобто з реально опублікованими `complete=true` барами, включно з tick-agg), щоб статус не показував фальшивий `DELAYED` при асинхронних публікаціях.
- History→OHLCV нормалізація: додано фільтр по `close_time` (майбутнє) — поточні незакриті бари з FXCM history відкидаються, щоб не спамити WARNING у `PublishDataGate` під час warmup/cache.
- Tick-agg: `volume` у live-превʼю (`complete=false`) рахується як tick volume (1.0 за кожен прийнятий тик), а закриті бари (`complete=true`) з tick-agg не публікуються назовні. Коли tick-agg внутрішньо закриває бар, стрім форсує FXCM history poll для відповідного `(symbol, tf)` (без очікування `publish_interval_seconds`), щоб якнайшвидше опублікувати complete=true свічку з «реальним» `Volume`.
- Tick-agg: додано калібрування preview-обсягу під FXCM history (`volume ≈ tick_count * k`, де `k` оцінюється по зіставлених барах з однаковим `open_time`). У heartbeat `context.tick_agg.volume_calibration` видно поточний `k`, кількість семплів та останню пару (history_volume vs tick_count).
- Debug viewer: додано режим `[9] VCAL`, який показує `heartbeat.context.tick_agg.volume_calibration` у вигляді таблиці (k, samples, остання пара history_volume/tick_count/ratio), а також короткі індикатори heartbeat/tick-agg (ts/age, tick age, clock flush age), щоб було видно чи UI реально оновлюється.
- Логи history→OHLCV: зменшено шум — відкидання поточного незакритого бару (close_time у майбутньому) тепер логиться як `DEBUG` з рейт-лімітом, а `WARNING` лишається лише для справжніх аномалій `open_time`.

## v2.4 (2025-12-16)

- `fxcm:status`: у блоці `session` додано `symbols` з per-(Symbol, TF) статистикою `High/Low/Avg` (те саме, що показує debug viewer у режимі Session), щоб консьюмери могли читати ці значення напряму зі статус-каналу.
- `fxcm_schema.py`: оновлено TypedDict-контракти для `fxcm:status.session.symbols` та додано runtime-валідацію `validate_status_payload_contract` + контрактний тест.
- VolumeCalibrator: зроблено адаптивне підбирання `k` (порівняння median vs L2 по MAPE) та додано в heartbeat `predicted_volume` і `err_pct` для останнього семпла, щоб було видно “як ми вчимось” і чи тримаємо цільову похибку.
- Debug viewer (VCAL): додано колонки `method`, `pred`, `err%` для швидкої оцінки точності калібрування.
- Cache: піднято `cache.max_bars` до 30000, щоб дефолтно вміщати 14d історії для 1m (20160 барів) і швидко віддавати warmup після рестарту SMC.
- History get_history: нормалізовано передавання таймфрейму у FXCM (`1h` → `H1`, `4h` → `H4` тощо) в усіх шляхах завантаження історії, щоб уникати порожніх відповідей/помилок через неправильний формат tf.
- Додано утиліту `tools/history_probe.py` для швидкої серверної перевірки, чи FXCM реально віддає history для `m15/m60/h4` (з тим самим мапінгом tf, що в конекторі).

## v2.5 (2025-12-16)

- MTF масштабування: стрім FXCM history тепер опитує лише базовий TF `1m` (FXCM=`m1`) по кожному символу, а старші TF для `complete=true` будуються локально з 1m історії.
- Додано публікацію `complete=true` старших TF (`5m/15m/1h/4h`) зі штампом `source=history_agg` (всередині барів і на рівні root payload), щоб downstream міг явно відрізняти «істину з history» від похідних.
- Tick preview (UI): додано агрегацію live-барів (complete=false) для `15m/1h/4h` (плюс виправлено пропуск публікації live-барів для `5m+`, які раніше рахувалися, але не віддавалися назовні).
- Warmup із кешу: прогрів/ensure_ready виконується лише для `1m`, а warmup старших TF публікується як `history_agg`, побудований з 1m кешу (без окремого FXCM history polling для кожного TF).
- Додано тест `tests/test_mtf_history_aggregation.py`, що перевіряє: (1) FXCM history викликається лише з `m1`, (2) у Redis з'являються `1h` бари, агреговані з 1m.
- Tick preview: зменшено кількість викликів `time.monotonic()` у `TickOhlcvWorker` при fanout на кілька TF (передаємо один `now_monotonic` через `_process_tick` → `_ingest_into_higher_preview_aggs` → `_publish_tick_bar`).
- Тести tick-preview: стабілізовано перевірки — після додавання публікації 5m/15m/1h/4h preview, вибір перевіряємого batch робиться по `timeframe=="1m"`, а не по `captured[-1]`.
- Прибрано попередження Pylance у `publish_ohlcv_to_redis` (pandas маски без `== True/False`) та у `VolumeCalibrator` (звуження Optional/Any без зміни логіки).
- VolumeCalibrator: для MTF tick-preview, якщо немає окремого `k` на старшому TF, використовується `k` з базового `1m` (tick_count агрегується лінійно).
- Додано тест, що перевіряє fallback калібрування `1m → 5m` preview.

## v2.6 (2025-12-17)

- Додано MTF cross-check: періодично звіряємо `history_agg` (агрегація з 1m) з прямим FXCM history для `1h/4h` і сигналізуємо `WARNING` при розбіжності понад допустимий поріг.
- Налаштування керуються через `config/runtime_settings.json` → `stream.mtf_crosscheck` (enabled, timeframes, min_interval_seconds, пороги для ціни/обʼєму).
- Додано тест `tests/test_mtf_crosscheck.py` для warning при розбіжності.
- Tick-agg: зроблено захист від падіння worker-а, якщо `ohlcv_sink` тимчасово не приймає батч (backpressure/помилка публікації) — тепер батч дропається, логується `DEBUG` і інкрементиться `PROM_ERROR_COUNTER(type="tick_agg_sink")`. Це знижує ризик, що після паузи ринку/Redis затримок UI перестає отримувати live-preview.
- Async supervisor: для черги `ohlcv` увімкнено `drop_oldest=True` при переповненні, щоб backpressure не робив `fxcm:ohlcv` повністю «глухим» (особливо критично для live-preview `complete=false`).
- Console status bar: додано `ohlcv_live_age` (час від останнього publish `complete=false`) та `ohlcv_pipe` (черга/drops/backpressure supervisor), щоб швидко відрізнити проблему FXCM тикового потоку від нашого пайплайна/Redis.

## v2.7 (2025-12-17)

- Додано готовий профіль для прогону на ~20 активах: `config/runtime_settings_20_assets.json` (40+ таргетів: 20 символів × `m1/m5`, плюс `XAU/USD:h1/h4` для MTF probe).

## v2.8 (2025-12-17)

- `connector.py`: виправлено проблеми типізації (Pylance) у формуванні `ohlcv_live_age_seconds` та парсингу `supervisor_ohlcv_queue/supervisor_ohlcv_dropped` (без зміни runtime-логіки).

## v2.9 (2025-12-19)

- S3 warmup: `fxcm_warmup` для `1h/4h` більше не ігнорується, якщо символ є в таргетах, але TF не прописаний у `stream_targets` — конектор прогріває 1m history у кеш за `lookback_minutes/min_history_bars`, локально будує `history_agg` і публікує MTF бари в `fxcm:ohlcv`.

## v3.0 (2025-12-19)

- S3 warmup/backfill для tick TF: `fxcm_warmup` та `fxcm_backfill` для `1m/5m` тепер публікують історичні бари в `fxcm:ohlcv` (root `source=history_s3`).
- Для `5m` історія будується як `history_agg`, похідна виключно від `1m` history (без прямого FXCM polling `m5`), тому правило "не змішувати" з tick-агрегацією зберігається.
- Live tick-preview як і раніше публікується лише як `complete=false` і не стає authoritative-джерелом.

## v3.1 (2025-12-19)

- Профіль [config/runtime_settings.json](config/runtime_settings.json): `cache.max_bars` піднято до 60000, щоб уміщати ~30d історії для `1m` (і мати запас для рестартів/догрузок).
