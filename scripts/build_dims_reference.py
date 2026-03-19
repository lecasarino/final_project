from common import ensure_warehouse, execute_sql, truncate_tables


def main():
    ensure_warehouse()
    truncate_tables(
        "mart.items_mart",
        "mart.orders_mart",
        "core.fact_order_driver_assignment",
        "core.fact_order_item",
        "core.fact_order",
        "core.dim_store",
        "core.dim_address",
        "core.dim_city",
        "core.dim_item",
        "core.dim_category",
        "core.dim_payment_type",
        "core.dim_cancellation_reason",
        restart_identity=True,
        cascade=True,
    )

    execute_sql("""
        INSERT INTO core.dim_category (category_name)
        SELECT DISTINCT item_category
        FROM staging.delivery_raw
        WHERE item_category IS NOT NULL;
    """)

    execute_sql("""
        INSERT INTO core.dim_payment_type (payment_type_name)
        SELECT DISTINCT payment_type
        FROM staging.delivery_raw
        WHERE payment_type IS NOT NULL;
    """)

    execute_sql("""
        INSERT INTO core.dim_cancellation_reason (
            cancellation_reason_name,
            is_service_fault
        )
        SELECT DISTINCT
            order_cancellation_reason,
            CASE
                WHEN order_cancellation_reason IN ('Ошибка приложения', 'Проблемы с оплатой')
                THEN TRUE ELSE FALSE
            END
        FROM staging.delivery_raw
        WHERE order_cancellation_reason IS NOT NULL;
    """)

    execute_sql("""
        INSERT INTO core.dim_city (city_name)
        SELECT DISTINCT trim(split_part(address_text, ',', 1))
        FROM staging.delivery_raw
        WHERE address_text IS NOT NULL;
    """)

    execute_sql("""
        INSERT INTO core.dim_address (city_id, full_address, address_type)
        SELECT DISTINCT
            c.city_id,
            r.address_text,
            'delivery'
        FROM staging.delivery_raw r
        JOIN core.dim_city c
          ON c.city_name = trim(split_part(r.address_text, ',', 1))
        WHERE r.address_text IS NOT NULL;
    """)

    execute_sql("""
        INSERT INTO core.dim_address (city_id, full_address, address_type)
        SELECT DISTINCT
            c.city_id,
            r.store_address,
            'store'
        FROM staging.delivery_raw r
        JOIN core.dim_city c
          ON c.city_name = trim(split_part(r.store_address, ',', 2))
        WHERE r.store_address IS NOT NULL;
    """)

    execute_sql("""
        INSERT INTO core.dim_store (store_id, store_address_id)
        SELECT DISTINCT
            r.store_id,
            a.address_id
        FROM staging.delivery_raw r
        JOIN core.dim_address a
          ON a.full_address = r.store_address
         AND a.address_type = 'store'
        WHERE r.store_id IS NOT NULL;
    """)

    execute_sql("""
        INSERT INTO core.dim_item (item_id, item_title, category_id)
        SELECT DISTINCT
            r.item_id,
            r.item_title,
            c.category_id
        FROM staging.delivery_raw r
        JOIN core.dim_category c
          ON c.category_name = r.item_category
        WHERE r.item_id IS NOT NULL;
    """)

    print("DIM REFERENCE READY")


if __name__ == "__main__":
    main()
