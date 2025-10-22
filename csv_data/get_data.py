import os
import pandas as pd
import subprocess
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

JOBS = [
    ("Number", r"E:"),
    ("Tractor", r"D:\Video"),
    ("Thomas",r"F:\Thomas"),
    ("Doll",r'F:\Doll'),
    ("Lolipop",r"\\MINGSEO2\Khay den")
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
    """Dùng ffprobe để lấy thời lượng video tính theo giây."""
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
    """Số giây đã trôi qua kể từ lần sửa đổi gần nhất (mtime)."""
    try:
        mtime = os.path.getmtime(file_path)
        now = datetime.now().timestamp()
        age = int(now - mtime)
        return age if age >= 0 else 0
    except Exception:
        return 0

def process_video(file_path, stt):
    duration_sec = get_video_duration_seconds(file_path)
    if duration_sec < MIN_DURATION_SECONDS:
        return None  # bỏ qua video ngắn hơn ngưỡng
    duration = format_duration(duration_sec)
    age_seconds = get_creation_age_seconds(file_path)
    # Giữ nguyên tên cột "lastest_used_value" cho tương thích pipeline cũ
    return [stt, file_path, duration, age_seconds]

def run_one_job(csv_name, folder_path):
    """Quét folder -> ghi csv_data/<csv_name>.csv"""
    print(f"\n=== Job: {csv_name} | Folder: {folder_path}")
    video_files = get_file_list(folder_path, VIDEO_EXTENSIONS)
    if not video_files:
        print("[INFO] No videos found.")
        return

    # Sắp xếp để STT ổn định theo tên file (có thể bỏ nếu muốn giữ nguyên thứ tự os.walk)
    video_files.sort(key=lambda p: os.path.basename(p).lower())

    output_file = os.path.join(CSV_OUTPUT_DIR, f"{csv_name}.csv")
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"[INFO] Deleted old CSV file: {output_file}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_video, fp, i+1): fp for i, fp in enumerate(video_files)}
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
        columns = ["stt", "file_path", "duration", "lastest_used_value"]
        df = pd.DataFrame(results, columns=columns)
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"[DONE] Saved to {output_file} ({len(df)} valid videos)")
    else:
        print("[INFO] No valid videos to save.")

def main():
    print("=== Video -> CSV (Batch via JOBS only) ===")
    if not JOBS:
        print("[ABORT] JOBS trống. Hãy khai báo JOBS trong file.")
        return

    for csv_name, folder_path in JOBS:
        folder_path = str(folder_path).strip()
        if not csv_name or not folder_path:
            print(f"[SKIP] Invalid job: ({csv_name!r}, {folder_path!r})")
            continue
        if not os.path.isdir(folder_path):
            print(f"[SKIP] Invalid folder: {folder_path}")
            continue
        run_one_job(csv_name, folder_path)

if __name__ == "__main__":
    main()
