import os
from contextlib import closing

import psycopg2


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "delivery_dwh")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


DDL_STATEMENTS = [
    "CREATE SCHEMA IF NOT EXISTS staging;",
    "CREATE SCHEMA IF NOT EXISTS core;",
    "CREATE SCHEMA IF NOT EXISTS mart;",
    """
    CREATE TABLE IF NOT EXISTS staging.delivery_raw (
        order_id BIGINT,
        user_id BIGINT,
        user_phone TEXT,
        driver_id BIGINT,
        driver_phone TEXT,
        address_text TEXT,
        store_id BIGINT,
        store_address TEXT,
        payment_type TEXT,
        order_cancellation_reason TEXT,
        created_at TIMESTAMP,
        paid_at TIMESTAMP,
        delivery_started_at TIMESTAMP,
        delivered_at TIMESTAMP,
        canceled_at TIMESTAMP,
        order_discount NUMERIC(10, 2),
        delivery_cost NUMERIC(10, 2),
        item_id BIGINT,
        item_title TEXT,
        item_category TEXT,
        item_quantity INTEGER,
        item_canceled_quantity INTEGER,
        item_price NUMERIC(10, 2),
        item_discount NUMERIC(10, 2),
        item_replaced_id BIGINT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_user (
        user_id BIGINT PRIMARY KEY,
        user_phone TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_driver (
        driver_id BIGINT PRIMARY KEY,
        driver_phone TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_category (
        category_id BIGSERIAL PRIMARY KEY,
        category_name TEXT NOT NULL UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_payment_type (
        payment_type_id BIGSERIAL PRIMARY KEY,
        payment_type_name TEXT NOT NULL UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_cancellation_reason (
        cancellation_reason_id BIGSERIAL PRIMARY KEY,
        cancellation_reason_name TEXT NOT NULL UNIQUE,
        is_service_fault BOOLEAN NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_city (
        city_id BIGSERIAL PRIMARY KEY,
        city_name TEXT NOT NULL UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_address (
        address_id BIGSERIAL PRIMARY KEY,
        city_id BIGINT NOT NULL REFERENCES core.dim_city (city_id),
        full_address TEXT NOT NULL,
        address_type TEXT NOT NULL,
        UNIQUE (full_address, address_type)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_store (
        store_id BIGINT PRIMARY KEY,
        store_address_id BIGINT NOT NULL REFERENCES core.dim_address (address_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.dim_item (
        item_id BIGINT PRIMARY KEY,
        item_title TEXT NOT NULL,
        category_id BIGINT NOT NULL REFERENCES core.dim_category (category_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.fact_order (
        order_id BIGINT PRIMARY KEY,
        user_id BIGINT REFERENCES core.dim_user (user_id),
        delivery_address_id BIGINT REFERENCES core.dim_address (address_id),
        store_id BIGINT REFERENCES core.dim_store (store_id),
        payment_type_id BIGINT REFERENCES core.dim_payment_type (payment_type_id),
        cancellation_reason_id BIGINT REFERENCES core.dim_cancellation_reason (cancellation_reason_id),
        created_at TIMESTAMP,
        paid_at TIMESTAMP,
        delivery_started_at TIMESTAMP,
        delivered_at TIMESTAMP,
        canceled_at TIMESTAMP,
        order_discount_pct NUMERIC(10, 2),
        delivery_cost NUMERIC(10, 2),
        is_canceled BOOLEAN,
        is_delivered BOOLEAN
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.fact_order_item (
        order_id BIGINT NOT NULL REFERENCES core.fact_order (order_id),
        item_id BIGINT NOT NULL REFERENCES core.dim_item (item_id),
        ordered_quantity INTEGER,
        canceled_quantity INTEGER,
        item_price NUMERIC(10, 2),
        item_discount_pct NUMERIC(10, 2),
        replaced_by_item_id BIGINT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS core.fact_order_driver_assignment (
        order_id BIGINT NOT NULL REFERENCES core.fact_order (order_id),
        driver_id BIGINT NOT NULL REFERENCES core.dim_driver (driver_id),
        assigned_at TIMESTAMP,
        unassigned_at TIMESTAMP,
        delivered_at_by_driver TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart.orders_mart (
        report_date DATE,
        year_num INTEGER,
        month_num INTEGER,
        day_num INTEGER,
        city_name TEXT,
        store_id BIGINT,
        gmv NUMERIC(18, 2),
        revenue NUMERIC(18, 2),
        profit NUMERIC(18, 2),
        created_orders_cnt BIGINT,
        delivered_orders_cnt BIGINT,
        canceled_orders_cnt BIGINT,
        canceled_after_delivery_cnt BIGINT,
        canceled_service_fault_cnt BIGINT,
        customers_cnt BIGINT,
        avg_check NUMERIC(18, 2),
        orders_per_customer NUMERIC(18, 4),
        revenue_per_customer NUMERIC(18, 2),
        courier_change_orders_cnt BIGINT,
        active_drivers_cnt BIGINT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mart.items_mart (
        report_date DATE,
        year_num INTEGER,
        month_num INTEGER,
        day_num INTEGER,
        city_name TEXT,
        store_id BIGINT,
        category_name TEXT,
        item_id BIGINT,
        item_title TEXT,
        item_gmv NUMERIC(18, 2),
        ordered_units BIGINT,
        canceled_units BIGINT,
        orders_with_item_cnt BIGINT,
        orders_with_item_cancellation_cnt BIGINT,
        popularity_rank_day BIGINT,
        popularity_rank_week BIGINT,
        popularity_rank_month BIGINT,
        anti_popularity_rank_day BIGINT,
        anti_popularity_rank_week BIGINT,
        anti_popularity_rank_month BIGINT
    );
    """,
]


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def execute_sql(sql: str) -> None:
    with closing(get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)


def ensure_warehouse() -> None:
    for statement in DDL_STATEMENTS:
        execute_sql(statement)


def truncate_tables(*tables: str, restart_identity: bool = False, cascade: bool = False) -> None:
    if not tables:
        return

    statement = "TRUNCATE TABLE " + ", ".join(tables)
    if restart_identity:
        statement += " RESTART IDENTITY"
    if cascade:
        statement += " CASCADE"
    statement += ";"
    execute_sql(statement)


def fetch_scalar(sql: str):
    with closing(get_connection()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
    return row[0] if row else None
