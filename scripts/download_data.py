from pathlib import Path

DATA_DIR = Path("/opt/airflow/data/raw")

def main():
    files = list(DATA_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("No parquet files found in /opt/airflow/data/raw")
    print(f"Found {len(files)} parquet files. Download step skipped.")

if __name__ == "__main__":
    main()