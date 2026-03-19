from common import ensure_warehouse, execute_sql


def main():
    ensure_warehouse()

    execute_sql("""
        INSERT INTO core.dim_user (user_id, user_phone)
        SELECT DISTINCT user_id, user_phone
        FROM staging.delivery_raw
        WHERE user_id IS NOT NULL
        ON CONFLICT (user_id) DO UPDATE
        SET user_phone = EXCLUDED.user_phone;
    """)

    execute_sql("""
        INSERT INTO core.dim_driver (driver_id, driver_phone)
        SELECT DISTINCT driver_id, driver_phone
        FROM staging.delivery_raw
        WHERE driver_id IS NOT NULL
        ON CONFLICT (driver_id) DO UPDATE
        SET driver_phone = EXCLUDED.driver_phone;
    """)

    print("DIM NATURAL READY")


if __name__ == "__main__":
    main()
