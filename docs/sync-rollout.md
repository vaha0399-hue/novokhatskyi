# P04 — пилоты синхронизации и бюджет

Проверка выполнена 2026-09-08 (UTC). Это выбор scope и расчёт бюджета для
последующих Q01–Q05; он не включает импорт, изменение БД, запуск worker или
включение сервисов. Артефакты прежних импортов использовались только для поиска
кандидатов. Итоговые ID, сезоны, coverage, расписание и структура ниже
перепроверены точечными read-only запросами к canonical БД и API-Football.

## Пилоты

| Роль | Турнир и тип | Canonical ID / season ID | API-Football ID / season | Проверенная структура и текущий календарь | Coverage и фактические данные | Доступная история и причина выбора |
| --- | --- | ---: | --- | --- | --- | --- |
| Лига | Premier League (England, `league`) | `3` / `14` | `39` / `2026` | `Regular Season - 1`…`38`; 380 fixtures, 30 completed; 2026-08-21…2027-05-30 | API: events, fixture statistics и standings — `true`. Сэмпл завершённого fixture вернул 2 team blocks и 18 типов метрик; таблица — 1 группа, 20 строк. В canonical БД статистика есть для 30 завершённых fixtures. | Provider: 17 сезонов, 2010…2026; в canonical БД: 2024…2026, 3 сезона. Базовый стабильный формат, полная таблица и текущий игровой цикл. |
| Кубок с групповой стадией | EFL Trophy (England, `cup`) | `168` / `179` | `46` / `2026` | `Group North - 1`…`8` и `Group South - 1`…`8`; 96 fixtures, 8 completed; 2026-08-18…2026-11-24 | API: events и fixture statistics — `true`; standings — `false`. Сэмпл статистики: 2 team blocks, 18 типов метрик; в canonical БД статистика есть для всех 8 завершённых fixtures. Отдельной standings snapshot нет. | Provider: 16 сезонов, 2011…2026; canonical БД: только 2026. Покрывает групповой формат и текущий сезон при доступной статистике. Для standings это явный режим **«данные недоступны»**, а не нулевая таблица. |
| Плей-офф | Copa Do Brasil (Brazil, `cup`) | `172` / `183` | `73` / `2026` | `1/256-finals`, `1/128-finals`, `Round of 64/32/16`, quarter-finals, semi-finals; 154 fixtures, 150 completed; 2026-02-17…2026-11-08 | API: events и fixture statistics — `true`; standings — `false`. Сэмпл статистики: 2 team blocks, 18 типов метрик; в canonical БД статистика есть для 150 завершённых fixtures. | Provider: 11 сезонов, 2016…2026; canonical БД: только 2026. Даёт прямой knockout с уже накопленными результатами и статистикой. Standings не запрашивать: coverage её не обещает. |

У EFL Trophy provider сообщает `start=end=2026-09-02` для сезона, но тот же
провайдер возвращает fixtures с 2026-08-18 по 2026-11-24. До исправления
поставщиком границы синхронизации этого scope следует определять по фактически
полученному расписанию, а не по этим двум полям сезона.

`coverage=true` означает, что provider заявляет поддержку league-season; это не
доказательство наличия каждого поля для каждого fixture. Пустая или частичная
статистика должна остаться отсутствующими данными согласно P03.

## Источники и способ проверки

Источник и дата проверки: 2026-09-08 (UTC).

- [API-Football v3: Leagues](https://www.api-football.com/documentation-v3#tag/Leagues/operation/get-leagues) — seasons и coverage по league-season.
- [API-Football v3: Fixtures](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures) и [официальное описание fixtures](https://www.api-football.com/news/post/how-to-get-all-fixtures-data-from-one-league) — расписание, статусы, round и fixture ID.
- [API-Football v3: Fixture statistics](https://www.api-football.com/documentation-v3#tag/Fixtures/operation/get-fixtures-statistics) и [официальное руководство](https://www.api-football.com/news/post/how-to-get-started-with-api-football-the-complete-beginners-guide) — статистика только при заявленном coverage; отсутствие не интерпретируется как ноль.
- [API-Football v3: Standings](https://www.api-football.com/documentation-v3#tag/Standings/operation/get-standings) — таблицы только при `coverage.standings=true`.
- [Ограничения API-Football](https://www.api-football.com/news/post/how-ratelimit-works) — дневные и минутные response headers.

Проверено без сохранения raw provider-ответов: `/status`; `/leagues?id=39,46,73`;
`/fixtures?league={id}&season=2026`; по одному
`/fixtures/statistics?fixture={completed fixture}` на пилот; и
`/standings?league=39&season=2026`. Ответы API держались только во временном
каталоге и удалялись после извлечения агрегатов. Canonical БД читалась только
через `SELECT` и системные `SHOW`.

## Ресурсы, лимиты и квота

| Категория | Наблюдение | Статус |
| --- | --- | --- |
| VPS | 2 vCPU; RAM 3.7 GiB, доступно 1.6 GiB; `/` свободно 38 GiB из 75 GiB; `/tmp` свободно 888 MiB из 1.9 GiB | Read-only снимок 2026-09-08; это не нагрузочный замер. |
| Remote PostgreSQL | `max_connections=60`, `superuser_reserved_connections=3`, на момент SELECT было 12 active connections | Фактический remote limit на момент проверки. Pooler и service limits провайдера отдельно не подтверждены. |
| Версионированная локальная конфигурация | `supabase/config.toml`: transaction pool, `default_pool_size=20`, `max_client_conn=100` | Настройка в репозитории, не доказательство текущей конфигурации hosted Supabase. |
| Настроенные caps импортёров | `current_season_statistics`: daily cap 5,500, максимум 90 запросов за запуск; шаблон catalogue service: daily cap 6,000, run cap 1,000, reserve 25 | Это конфигурационные/кодовые значения. Единого работающего quota governor ещё нет (задача Q04). |
| Фактический аккаунт API-Football | `/status`: активный `Pro`, дневной лимит 7,500; headers: 300/min и 7,491 запрос остаётся | Наблюдение во время проверки; дата окончания подписки и идентифицирующие поля аккаунта намеренно не публикуются. |

Настроенные caps, фактические provider limits и предложенный ниже бюджет — разные
величины. Ни один существующий локальный cap не является доказательством
глобального enforcement между будущими workers.

## Предлагаемый бюджет

### Первичная загрузка ограниченной истории

Начальный scope намеренно ограничен тем, что уже есть в canonical БД: Premier
League 2024–2026, EFL Trophy 2026 и Copa do Brasil 2026. Это 948 завершённых
fixtures с уже наблюдавшейся парой team statistics. Полный архив provider
(17 + 16 + 11 сезонов) в P04 не включается: его объём и пригодность для
аналитики должны быть отдельно утверждены.

| Составляющая | Расчёт | Запросы |
| --- | ---: | ---: |
| Discovery и coverage | 3 турнира × 1 `/leagues` | 3 |
| Расписание и результаты | 5 canonical сезонов × 1 `/fixtures?league&season` | 5 |
| Standings | Premier League 2026 | 1 |
| Историческая fixture statistics | 948 completed fixtures × 1 `/fixtures/statistics` | 948 |
| Контролируемые повторы | текущий `statistics_backfill` допускает до 5 глобальных повторов | 5 |
| Подытог |  | 962 |
| Запас 10% на контролируемые ошибки/повторы | `ceil(962 × 0.10)` | 97 |
| **Итого для initial scope** |  | **1,059** |

Расчёт статистики консервативный: существующий historical backfill обращается к
`/fixtures/statistics` по одному fixture, хотя current-season path умеет
паковать до 20 IDs. В historical backfill есть лимит 385 попыток на запуск,
поэтому кампанию следует разбить как минимум на три checkpointed run; это
планирование, не команда на запуск. 1,059 меньше версии daily cap 6,000 и
фактического лимита 7,500, но запускать его можно только после Q04, когда
общий лимит станет атомарно контролируемым.

### Ежедневный бюджет трёх пилотов

| Составляющая | Консервативное допущение | Запросов/сутки |
| --- | --- | ---: |
| Calendar, results и pre/post-match checks | 3 scope × (8 near + 1 far) = 27; ещё до 48 проверок около kick-off и terminal response | 75 |
| Standings | Premier League раз в час в игровой день; у обоих cup coverage `false` | 24 |
| Статистика | первые попытки и запланированные повторы для новых terminal fixtures | 60 |
| Correction/discovery sweep | исправления +24/+72 часа и небольшой запас | 41 |
| **Обычный sync без live** | округлённый потолок | **200** |
| Live reserve | единый poll каждые 25 секунд в окне до 10 часов: `10×3600/25` | **1,440** |
| Повторы и исправления при всплеске | отдельный лимит сверх обычного расчёта | **360** |
| **Операционный потолок** | `200 + 1,440 + 360` | **2,000** |
| Удерживаемый резерв до внутреннего cap | `6,000 − 2,000` | **4,000** |

Live-расчёт предполагает один общий live poll с локальной фильтрацией трёх
scope. Если будущая реализация потребует отдельный poll на каждый tournament,
live часть вырастет до 4,320 запросов/сутки и должна быть пересчитана до
включения. Даже оценочный потолок 2,000/сутки и 25-секундный poll существенно
ниже наблюдённых 7,500/сутки и 300/min; это совместимость бюджета, а не
доказательство пропускной способности worker или provider.

## Совместимость с целями раздела 9

| Цель | Вывод P04 |
| --- | --- |
| Live p95 ≤ 60 s | Бюджет 25-секундного poll оставляет запас по quota. Цель не измерялась: нет запущенного scheduler, очереди и latency-метрик. |
| Result p95 ≤ 15 min; statistics first attempt p95 ≤ 15 min; aggregates p95 ≤ 5 min | Бюджет оставляет запросы для проверок и повторов, но Q01–Q05 и runtime-измерения ещё отсутствуют. Цели не достигнуты и не могут считаться подтверждёнными. |
| Standings freshness ≤ 90 min | Совместима только с Premier League: hourly budget — 24 запроса. Для EFL Trophy и Copa do Brasil coverage `false`, поэтому таблицы не создаются и эта цель к ним неприменима. |
| Scanner API p95 ≤ 1 s при 20 клиентах | Не подтверждено: 2 vCPU и 1.6 GiB доступной RAM не являются нагрузочным доказательством. Нужен отдельный сценарий с зафиксированным dataset и cold/warm состоянием. |
| Один реальный игровой цикл каждого формата | Календарь покрывает активную лигу, group cup и knockout cup, но цикл не запускался. Это будущая canary-проверка R03. |

## Существенные ограничения

- Текущий provider season interval EFL Trophy противоречит возвращённому им же
  календарю; использовать fixtures как источник окна до повторной проверки.
- У двух cup provider не заявляет standings. Не заменять отсутствие данными
  другой сущности и не считать его пустой таблицей.
- Остаток квоты и состояние подписки быстро меняются; перед любым запуском
  повторить `/status` и учесть response headers, не это значение из документа.
- P04 не создаёт policy registry, общий quota governor, очередь, scheduler,
  raw-retention процесс или production измерения. Они остаются работами Q01–Q05
  и R03.
