# Delivery DWH Pipeline

## Описание проекта

В рамках проекта реализован **end-to-end аналитический DWH pipeline**
для сервиса доставки.

Pipeline выполняет:

-   загрузку сырых данных
-   построение витрин хранения (DWH Core)
-   расчёт аналитических мартов
-   orchestration через Apache Airflow

Проект построен по классической **многослойной архитектуре хранилища
данных (Data Warehouse)**.

------------------------------------------------------------------------

## Архитектура решения

Pipeline реализует следующую архитектуру:

RAW → STAGING → CORE (DWH) → MARTS

### RAW layer

Источник данных --- parquet-файлы с заказами.

Загрузка выполняется скриптом:

scripts/download_data.py

Файлы сохраняются в:

data/raw/

------------------------------------------------------------------------

### STAGING layer

Сырые данные загружаются в таблицу:

staging.orders_raw

Особенности:

-   структура максимально близка к источнику
-   слой используется как landing zone
-   данные перезаписываются при каждом запуске pipeline

------------------------------------------------------------------------

### CORE layer (нормализованное DWH)

На этом этапе происходит **нормализация данных и построение модели
"звезда"**.

Создаются:

#### Natural dimensions

-   core.dim_user
-   core.dim_driver
-   core.dim_store
-   core.dim_item

#### Reference dimensions

-   core.dim_category
-   core.dim_city
-   core.dim_payment_type

#### Fact table

-   core.fact_order

Загрузка выполняется idempotent‑подходом (UPSERT), что позволяет
безопасно перезапускать pipeline.

------------------------------------------------------------------------

### MART layer

Финальные аналитические витрины:

-   mart.orders_mart --- агрегаты по заказам
-   mart.items_mart --- агрегаты по товарам

Используются для BI / аналитики.

------------------------------------------------------------------------

## Orchestration

Pipeline управляется через **Apache Airflow DAG**

Файл:

dags/delivery_pipeline_dag.py

Последовательность задач:

download_data → load_to_staging → cleanup_pipeline → build_dims_natural
→ build_dims_reference → build_facts → build_orders_mart →
build_items_mart

------------------------------------------------------------------------

## 🐳 Запуск проекта

### 1. Клонировать репозиторий

git clone `<repo_url>`{=html} cd final_project

### 2. Запустить контейнеры

docker compose up --build

### 3. Открыть Airflow

http://localhost:8080

Логин: airflow\
Пароль: airflow

### 4. Запустить DAG

Открыть DAG `delivery_pipeline` → нажать **Trigger DAG**

------------------------------------------------------------------------

## Стек

-   Python
-   PySpark
-   PostgreSQL
-   Apache Airflow
-   Docker

------------------------------------------------------------------------

## Результат

-   построено нормализованное DWH
-   реализована star schema
-   подготовлены аналитические витрины
-   pipeline полностью воспроизводим
