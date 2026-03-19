from common import ensure_warehouse, execute_sql, fetch_scalar, truncate_tables


def main():
    ensure_warehouse()

    print("===== CLEAR FACT TABLES =====")
    truncate_tables(
        "core.fact_order_driver_assignment",
        "core.fact_order_item",
        "core.fact_order",
    )

    print("===== BUILD fact_order =====")
    execute_sql("""
        INSERT INTO core.fact_order (
            order_id,
            user_id,
            delivery_address_id,
            store_id,
            payment_type_id,
            cancellation_reason_id,
            created_at,
            paid_at,
            delivery_started_at,
            delivered_at,
            canceled_at,
            order_discount_pct,
            delivery_cost,
            is_canceled,
            is_delivered
        )
        SELECT
            r.order_id,
            MAX(r.user_id) AS user_id,
            MAX(a.address_id) AS delivery_address_id,
            MAX(r.store_id) AS store_id,
            MAX(pt.payment_type_id) AS payment_type_id,
            MAX(cr.cancellation_reason_id) AS cancellation_reason_id,
            MIN(r.created_at) AS created_at,
            MAX(r.paid_at) AS paid_at,
            MIN(r.delivery_started_at) AS delivery_started_at,
            MAX(r.delivered_at) AS delivered_at,
            MAX(r.canceled_at) AS canceled_at,
            MAX(COALESCE(r.order_discount, 0)) AS order_discount_pct,
            MAX(COALESCE(r.delivery_cost, 0)) AS delivery_cost,
            CASE WHEN MAX(r.canceled_at) IS NOT NULL THEN TRUE ELSE FALSE END AS is_canceled,
            CASE WHEN MAX(r.delivered_at) IS NOT NULL THEN TRUE ELSE FALSE END AS is_delivered
        FROM staging.delivery_raw r
        JOIN core.dim_address a
          ON a.full_address = r.address_text
         AND a.address_type = 'delivery'
        LEFT JOIN core.dim_payment_type pt
          ON pt.payment_type_name = r.payment_type
        LEFT JOIN core.dim_cancellation_reason cr
          ON cr.cancellation_reason_name = r.order_cancellation_reason
        WHERE r.order_id IS NOT NULL
        GROUP BY r.order_id;
    """)

    print("===== BUILD fact_order_item =====")
    execute_sql("""
        INSERT INTO core.fact_order_item (
            order_id,
            item_id,
            ordered_quantity,
            canceled_quantity,
            item_price,
            item_discount_pct,
            replaced_by_item_id
        )
        SELECT
            r.order_id,
            r.item_id,
            COALESCE(r.item_quantity, 0)::int,
            COALESCE(r.item_canceled_quantity, 0)::int,
            COALESCE(r.item_price, 0),
            COALESCE(r.item_discount, 0),
            CASE
                WHEN r.item_replaced_id IS NULL THEN NULL
                ELSE r.item_replaced_id::bigint
            END AS replaced_by_item_id
        FROM staging.delivery_raw r
        WHERE r.order_id IS NOT NULL
          AND r.item_id IS NOT NULL;
    """)

    print("===== BUILD fact_order_driver_assignment =====")
    execute_sql("""
        INSERT INTO core.fact_order_driver_assignment (
            order_id,
            driver_id,
            assigned_at,
            unassigned_at,
            delivered_at_by_driver
        )
        SELECT DISTINCT
            r.order_id,
            r.driver_id,
            r.delivery_started_at AS assigned_at,
            CASE
                WHEN r.delivered_at IS NULL AND r.canceled_at IS NOT NULL THEN r.canceled_at
                ELSE NULL
            END AS unassigned_at,
            CASE
                WHEN r.delivered_at IS NOT NULL THEN r.delivered_at
                ELSE NULL
            END AS delivered_at_by_driver
        FROM staging.delivery_raw r
        WHERE r.order_id IS NOT NULL
          AND r.driver_id IS NOT NULL;
    """)

    print("===== FACT BUILD FINISHED =====")

    for table in [
        "core.fact_order",
        "core.fact_order_item",
        "core.fact_order_driver_assignment",
    ]:
        cnt = fetch_scalar(f"SELECT COUNT(*) FROM {table};")
        print(f"{table}: {cnt}")


if __name__ == "__main__":
    main()
