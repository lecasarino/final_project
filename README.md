# Delivery DWH Project

#Описание проекта

В рамках проекта реализовано построение аналитического хранилища данных (DWH) для сервиса доставки.

Pipeline реализует полный цикл обработки данных:

RAW → STAGING → CORE → MART

Используемый стек:
	•	Docker
	•	PostgreSQL
	•	PySpark
	•	Airflow

#Архитектура

Проект реализует классическую слоистую архитектуру DWH:

RAW (parquet)
     ↓
STAGING (landing таблица)
     ↓
CORE (нормализованная модель)
     ↓
MART (витрины)


#Структура репозитория
final_project/
│
├── dags/
│   └── delivery_pipeline_dag.py
│
├── scripts/
│   ├── download_data.py
│   ├── load_to_staging.py
│   ├── normalize_to_core.py
│   ├── build_facts.py
│   ├── build_orders_mart.py
│   └── build_items_mart.py
│
├── sql/
│   ├── staging/
│   │   └── create_staging.sql
│   ├── core/
│   │   └── create_core.sql
│   └── mart/
│       └── create_mart.sql
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md


#Слои DWH

#RAW
Хранение исходных parquet файлов.

------------------------------

#STAGING
Таблица staging.delivery_raw
Хранит полный денормализованный снапшот заказов.

------------------------------

#CORE

Нормализованная модель:
	•	dim_user
	•	dim_driver
	•	dim_store
	•	dim_item
	•	fact_order
	•	fact_order_item

------------------------------

#MART

Построены витрины:

mart.orders_mart

Метрики:
	•	total_orders
	•	completed_orders
	•	canceled_orders
	•	revenue
	•	avg_delivery_time

Гранулярность: день × магазин
------------------------------
mart.items_mart

Метрики:
	•	total_items
	•	canceled_items
	•	revenue

Гранулярность: день × товар
------------------------------
#Запуск проекта

1. Сборка контейнеров
   docker compose up --build

2. Airflow UI
   http://localhost:8080
   Логин: airflow
   Пароль: airflow

3. Запуск DAG
   DAG: delivery_pipeline
   Pipeline выполняет:
  	1.	download parquet
  	2.	load to staging
  	3.	normalize to core
  	4.	build facts
  	5.	build marts

#Особенности
	•	Проект рассчитан на локальное выполнение
	•	Объем данных ~10 млн строк
	•	Возможны долгие выполнения normalize_to_core при ограничениях RAM Docker

Pipeline является идемпотентным, воспроизводимым, полностью автоматизированным через Airflow


# Использование витрин

После выполнения DAG можно выполнять аналитические запросы:
select *
from mart.orders_mart
order by date desc;

#Цель проекта

Продемонстрировать навыки:
	•	построения DWH
	•	нормализации данных
	•	построения витрин
	•	orchestration через Airflow
	•	работы с PySpark
	•	контейнеризации

