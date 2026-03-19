from common import ensure_warehouse, execute_sql, truncate_tables


def main():
    ensure_warehouse()
    truncate_tables("mart.orders_mart")

    execute_sql("""
        INSERT INTO mart.orders_mart (
            report_date,
            year_num,
            month_num,
            day_num,
            city_name,
            store_id,
            gmv,
            revenue,
            profit,
            created_orders_cnt,
            delivered_orders_cnt,
            canceled_orders_cnt,
            canceled_after_delivery_cnt,
            canceled_service_fault_cnt,
            customers_cnt,
            avg_check,
            orders_per_customer,
            revenue_per_customer,
            courier_change_orders_cnt,
            active_drivers_cnt
        )
        SELECT
            fo.created_at::date AS report_date,
            EXTRACT(YEAR FROM fo.created_at)::int AS year_num,
            EXTRACT(MONTH FROM fo.created_at)::int AS month_num,
            EXTRACT(DAY FROM fo.created_at)::int AS day_num,
            c.city_name,
            fo.store_id,

            COALESCE(SUM(item_metrics.gmv), 0) AS gmv,
            COALESCE(SUM(item_metrics.revenue), 0) AS revenue,
            COALESCE(SUM(item_metrics.revenue), 0) - COALESCE(SUM(fo.delivery_cost), 0) AS profit,

            COUNT(DISTINCT fo.order_id) AS created_orders_cnt,
            COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.is_delivered) AS delivered_orders_cnt,
            COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.is_canceled) AS canceled_orders_cnt,

            COUNT(DISTINCT fo.order_id) FILTER (
                WHERE fo.is_canceled AND fo.delivered_at IS NOT NULL
            ) AS canceled_after_delivery_cnt,

            COUNT(DISTINCT fo.order_id) FILTER (
                WHERE fo.is_canceled AND dcr.is_service_fault
            ) AS canceled_service_fault_cnt,

            COUNT(DISTINCT fo.user_id) AS customers_cnt,

            COALESCE(SUM(item_metrics.revenue), 0)
                / NULLIF(COUNT(DISTINCT fo.order_id) FILTER (WHERE fo.is_delivered), 0) AS avg_check,

            COUNT(DISTINCT fo.order_id)::numeric
                / NULLIF(COUNT(DISTINCT fo.user_id), 0) AS orders_per_customer,

            COALESCE(SUM(item_metrics.revenue), 0)
                / NULLIF(COUNT(DISTINCT fo.user_id), 0) AS revenue_per_customer,

            COUNT(DISTINCT fo.order_id) FILTER (
                WHERE driver_stats.driver_cnt > 1
            ) AS courier_change_orders_cnt,

            COUNT(DISTINCT fda.driver_id) AS active_drivers_cnt

        FROM core.fact_order fo

        JOIN core.dim_address da
          ON da.address_id = fo.delivery_address_id

        JOIN core.dim_city c
          ON c.city_id = da.city_id

        LEFT JOIN core.dim_cancellation_reason dcr
          ON dcr.cancellation_reason_id = fo.cancellation_reason_id

        LEFT JOIN (
            SELECT
                foi.order_id,
                SUM(
                    foi.ordered_quantity
                    * foi.item_price
                    * (1 - foi.item_discount_pct / 100.0)
                ) AS gmv,
                SUM(
                    (foi.ordered_quantity - foi.canceled_quantity)
                    * foi.item_price
                    * (1 - foi.item_discount_pct / 100.0)
                ) AS revenue
            FROM core.fact_order_item foi
            GROUP BY foi.order_id
        ) item_metrics
          ON item_metrics.order_id = fo.order_id

        LEFT JOIN core.fact_order_driver_assignment fda
          ON fda.order_id = fo.order_id

        LEFT JOIN (
            SELECT
                order_id,
                COUNT(DISTINCT driver_id) AS driver_cnt
            FROM core.fact_order_driver_assignment
            GROUP BY order_id
        ) driver_stats
          ON driver_stats.order_id = fo.order_id

        GROUP BY
            fo.created_at::date,
            EXTRACT(YEAR FROM fo.created_at)::int,
            EXTRACT(MONTH FROM fo.created_at)::int,
            EXTRACT(DAY FROM fo.created_at)::int,
            c.city_name,
            fo.store_id;
    """)

    print("ORDERS MART READY")


if __name__ == "__main__":
    main()
