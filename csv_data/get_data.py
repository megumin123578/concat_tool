import os
import pandas as pd
import subprocess
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

JOBS = [
    ("Number", [r"E:\Number A\Video", r"E:\Number B\Video", r"E:\Number SLime\Video", r"E:\Number TC\Video", r"E:\Rainbow Number\Video"]),
    ("Tractor", [r"D:\Video"]),
    ("Thomas", [r"F:\Thomas"]),
    ("Doll", [r"F:\Doll"]),
    ("Lolipop", [r"\\MINGSEO2\Khay den"])
]

VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mkv', '.mov'}
CSV_OUTPUT_DIR = "csv_data"
MIN_DURATION_SECONDS = 60
MAX_WORKERS = 8

os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)

def get_file_list(folder_path, extensions=None):
    try:
        if not os.path.exists(folder_path):
            print(f"[WARN] Folder '{folder_path}' does not exist.")
            return []
        file_list = []
        for root, _, files in os.walk(folder_path):
            for item in files:
                if extensions is None or os.path.splitext(item)[1].lower() in extensions:
                    file_list.append(os.path.abspath(os.path.join(root, item)))
        return file_list
    except Exception as e:
        print(f"[ERR] Error accessing folder '{folder_path}': {e}")
        return []

def get_video_duration_seconds(file_path):
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "json", file_path
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        data = json.loads(result.stdout or "{}")
        return float(data.get("format", {}).get("duration", 0.0))
    except Exception:
        return 0.0

def format_duration(seconds):
    minute = int(seconds // 60)
    sec = int(seconds % 60)
    return f"{minute}:{sec:02d}"

def get_creation_age_seconds(file_path):
    try:
        mtime = os.path.getmtime(file_path)
        now = datetime.now().timestamp()
        age = int(now - mtime)
        return age if age >= 0 else 0
    except Exception:
        return 0

def is_valid_video(file_path):
    """Trả về True nếu video >= MIN_DURATION_SECONDS."""
    return get_video_duration_seconds(file_path) >= MIN_DURATION_SECONDS

def process_video(file_path, stt):
    duration_sec = get_video_duration_seconds(file_path)
    if duration_sec < MIN_DURATION_SECONDS:
        return None
    duration = format_duration(duration_sec)
    age_seconds = get_creation_age_seconds(file_path)
    return [stt, file_path, duration, age_seconds]

def run_one_job(csv_name, folder_paths):
    print(f"\n=== Job: {csv_name} ===")
    all_videos = []
    for path in folder_paths:
        videos = get_file_list(path, VIDEO_EXTENSIONS)
        print(f"[INFO] Found {len(videos)} videos in {path}")
        all_videos.extend(videos)

    if not all_videos:
        print("[INFO] No videos found.")
        return

    # === Đếm số video hợp lệ (>=60s) ===
    valid_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(is_valid_video, fp): fp for fp in all_videos}
        for future in as_completed(futures):
            if future.result():
                valid_count += 1

    output_file = os.path.join(CSV_OUTPUT_DIR, f"{csv_name}.csv")

    # === Kiểm tra nếu CSV cũ có cùng số lượng hợp lệ ===
    if os.path.exists(output_file):
        try:
            old_df = pd.read_csv(output_file)
            old_count = len(old_df)
            if old_count == valid_count:
                print(f"[SKIP] {csv_name}: same valid count ({valid_count} videos).")
                return
            else:
                print(f"[INFO] Valid count changed: old={old_count}, new={valid_count}. Updating...")
        except Exception as e:
            print(f"[WARN] Cannot read old CSV ({e}), will rebuild.")

    # === Thực sự xử lý và lưu CSV ===
    all_videos.sort(key=lambda p: os.path.basename(p).lower())
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_video, fp, i+1): fp for i, fp in enumerate(all_videos)}
        for idx, future in enumerate(as_completed(futures), 1):
            fp = futures[future]
            try:
                row = future.result()
                if row:
                    results.append(row)
                    print(f"{idx}: {row[1]}")
                else:
                    print(f"{idx}: Skipped (<{MIN_DURATION_SECONDS}s): {fp}")
            except Exception as e:
                print(f"[ERR] {fp}: {e}")

    if results:
        df = pd.DataFrame(results, columns=["stt", "file_path", "duration", "lastest_used_value"])
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"[DONE] Saved to {output_file} ({len(df)} valid videos)")
    else:
        print("[INFO] No valid videos to save.")

def main():
    print("=== Video → CSV (Skip if same valid count) ===")
    for csv_name, paths in JOBS:
        if isinstance(paths, str):
            paths = [paths]
        valid_paths = [p for p in paths if os.path.isdir(p)]
        if not valid_paths:
            print(f"[SKIP] Invalid paths for {csv_name}: {paths}")
            continue
        run_one_job(csv_name, valid_paths)

if __name__ == "__main__":
    main()
