from common import ensure_warehouse, execute_sql, truncate_tables


def main():
    ensure_warehouse()
    truncate_tables("mart.items_mart")

    execute_sql("""
        INSERT INTO mart.items_mart (
            report_date,
            year_num,
            month_num,
            day_num,
            city_name,
            store_id,
            category_name,
            item_id,
            item_title,
            item_gmv,
            ordered_units,
            canceled_units,
            orders_with_item_cnt,
            orders_with_item_cancellation_cnt,
            popularity_rank_day,
            popularity_rank_week,
            popularity_rank_month,
            anti_popularity_rank_day,
            anti_popularity_rank_week,
            anti_popularity_rank_month
        )
        WITH daily_base AS (
            SELECT
                fo.created_at::date AS report_date,
                EXTRACT(YEAR FROM fo.created_at)::int AS year_num,
                EXTRACT(MONTH FROM fo.created_at)::int AS month_num,
                EXTRACT(DAY FROM fo.created_at)::int AS day_num,
                date_trunc('week', fo.created_at)::date AS week_start,
                date_trunc('month', fo.created_at)::date AS month_start,
                c.city_name,
                fo.store_id,
                dc.category_name,
                di.item_id,
                di.item_title,

                SUM(
                    foi.ordered_quantity
                    * foi.item_price
                    * (1 - foi.item_discount_pct / 100.0)
                ) AS item_gmv,

                SUM(foi.ordered_quantity) AS ordered_units,
                SUM(foi.canceled_quantity) AS canceled_units,

                COUNT(DISTINCT fo.order_id) AS orders_with_item_cnt,
                COUNT(DISTINCT fo.order_id) FILTER (
                    WHERE foi.canceled_quantity > 0
                ) AS orders_with_item_cancellation_cnt
            FROM core.fact_order_item foi
            JOIN core.fact_order fo
              ON fo.order_id = foi.order_id
            JOIN core.dim_item di
              ON di.item_id = foi.item_id
            JOIN core.dim_category dc
              ON dc.category_id = di.category_id
            JOIN core.dim_address da
              ON da.address_id = fo.delivery_address_id
            JOIN core.dim_city c
              ON c.city_id = da.city_id
            GROUP BY
                fo.created_at::date,
                EXTRACT(YEAR FROM fo.created_at)::int,
                EXTRACT(MONTH FROM fo.created_at)::int,
                EXTRACT(DAY FROM fo.created_at)::int,
                date_trunc('week', fo.created_at)::date,
                date_trunc('month', fo.created_at)::date,
                c.city_name,
                fo.store_id,
                dc.category_name,
                di.item_id,
                di.item_title
        ),
        week_rank_base AS (
            SELECT
                week_start,
                city_name,
                store_id,
                item_id,
                DENSE_RANK() OVER (
                    PARTITION BY week_start, city_name, store_id
                    ORDER BY SUM(ordered_units) DESC, item_id
                ) AS popularity_rank_week,
                DENSE_RANK() OVER (
                    PARTITION BY week_start, city_name, store_id
                    ORDER BY SUM(ordered_units) ASC, item_id
                ) AS anti_popularity_rank_week
            FROM daily_base
            GROUP BY week_start, city_name, store_id, item_id
        ),
        month_rank_base AS (
            SELECT
                month_start,
                city_name,
                store_id,
                item_id,
                DENSE_RANK() OVER (
                    PARTITION BY month_start, city_name, store_id
                    ORDER BY SUM(ordered_units) DESC, item_id
                ) AS popularity_rank_month,
                DENSE_RANK() OVER (
                    PARTITION BY month_start, city_name, store_id
                    ORDER BY SUM(ordered_units) ASC, item_id
                ) AS anti_popularity_rank_month
            FROM daily_base
            GROUP BY month_start, city_name, store_id, item_id
        )
        SELECT
            db.report_date,
            db.year_num,
            db.month_num,
            db.day_num,
            db.city_name,
            db.store_id,
            db.category_name,
            db.item_id,
            db.item_title,
            db.item_gmv,
            db.ordered_units,
            db.canceled_units,
            db.orders_with_item_cnt,
            db.orders_with_item_cancellation_cnt,

            DENSE_RANK() OVER (
                PARTITION BY db.report_date, db.city_name, db.store_id
                ORDER BY db.ordered_units DESC, db.item_id
            ) AS popularity_rank_day,

            wr.popularity_rank_week,
            mr.popularity_rank_month,

            DENSE_RANK() OVER (
                PARTITION BY db.report_date, db.city_name, db.store_id
                ORDER BY db.ordered_units ASC, db.item_id
            ) AS anti_popularity_rank_day,

            wr.anti_popularity_rank_week,
            mr.anti_popularity_rank_month

        FROM daily_base db
        LEFT JOIN week_rank_base wr
          ON wr.week_start = db.week_start
         AND wr.city_name = db.city_name
         AND wr.store_id = db.store_id
         AND wr.item_id = db.item_id
        LEFT JOIN month_rank_base mr
          ON mr.month_start = db.month_start
         AND mr.city_name = db.city_name
         AND mr.store_id = db.store_id
         AND mr.item_id = db.item_id;
    """)

    print("ITEMS MART READY")


if __name__ == "__main__":
    main()
