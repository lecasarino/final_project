import os
import glob
import gdown

FOLDER_URL = "https://drive.google.com/drive/folders/1h8bwnfuwpQ6oPL_ZjW9H9eM6GvG0OYq0?usp=sharing"
OUTPUT_DIR = "/opt/project_data/raw"


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    existing = glob.glob(os.path.join(OUTPUT_DIR, "*.parquet"))
    if existing:
        print(f"Parquet files already exist: {len(existing)} files")
        for f in existing:
            print(f" - {os.path.basename(f)}")
        return

    print("Downloading parquet files from Google Drive...")
    gdown.download_folder(
        url=FOLDER_URL,
        output=OUTPUT_DIR,
        quiet=False,
        use_cookies=False
    )

    files = glob.glob(os.path.join(OUTPUT_DIR, "*.parquet"))
    print(f"Downloaded {len(files)} parquet files")
    for f in files:
        print(f" - {os.path.basename(f)}")


if __name__ == "__main__":
    main()
