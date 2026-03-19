from common import ensure_warehouse, truncate_tables


def main():
    ensure_warehouse()
    truncate_tables(
        "mart.items_mart",
        "mart.orders_mart",
        "core.fact_order_driver_assignment",
        "core.fact_order_item",
        "core.fact_order",
    )

    print("PIPELINE CLEANUP READY")


if __name__ == "__main__":
    main()
