import glob

from pyspark.sql import SparkSession

from common import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    ensure_warehouse,
    truncate_tables,
)

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

PARQUET_PATH = "/opt/project_data/raw/*.parquet"
def main():
    print("START load_to_staging")
    ensure_warehouse()

    parquet_files = glob.glob(PARQUET_PATH)
    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet files found at {PARQUET_PATH}. Run download_data.py first."
        )

    truncate_tables("staging.delivery_raw")
    print("staging.delivery_raw truncated")

    spark = (
        SparkSession.builder
        .appName("load_to_staging")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    df = spark.read.parquet(PARQUET_PATH)
    print("PARQUET READ OK")

    df.write.jdbc(
        url=POSTGRES_URL,
        table="staging.delivery_raw",
        mode="append",
        properties=POSTGRES_PROPERTIES,
    )

    print("LOAD FINISHED")
    spark.stop()


if __name__ == "__main__":
    main()
