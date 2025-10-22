import os
import pandas as pd
import subprocess
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Cấu hình ===
VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mkv', '.mov'}
os.makedirs("csv_data", exist_ok=True)
input_csv = input("Enter CSV name: ").strip()
OUTPUT_FILE = f"csv_data/{input_csv}.csv"

# === Hàm hỗ trợ ===
def get_file_list(folder_path, extensions=None):
    try:
        if not os.path.exists(folder_path):
            print(f"Folder '{folder_path}' does not exist.")
            return []
        file_list = []
        for root, _, files in os.walk(folder_path):
            for item in files:
                if extensions is None or os.path.splitext(item)[1].lower() in extensions:
                    file_list.append(os.path.abspath(os.path.join(root, item)))
        return file_list
    except Exception as e:
        print(f"Error accessing folder '{folder_path}': {e}")
        return []

def get_video_duration_ffprobe(file_path):
    """Dùng ffprobe để lấy thời lượng video (nhanh gấp 10-20 lần MoviePy)"""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "json", file_path
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        data = json.loads(result.stdout)
        duration = float(data["format"]["duration"])
        minute = int(duration // 60)
        sec = int(duration % 60)
        return f"{minute}:{sec:02d}"
    except Exception:
        return "0:00"

def get_creation_time(file_path):
    try:
        ctime = os.path.getmtime(file_path)
        now = datetime.now().timestamp()
        return int(now - ctime)
    except Exception:
        return 0

def get_last_stt(output_file):
    try:
        if not os.path.exists(output_file):
            return 0
        df = pd.read_csv(output_file, encoding="utf-8-sig")
        return int(df["stt"].max()) if "stt" in df.columns and not df.empty else 0
    except Exception:
        return 0

def process_video(file_path, stt):
    duration = get_video_duration_ffprobe(file_path)
    creation_time = get_creation_time(file_path)
    return [stt, file_path, duration, creation_time]

def run_mode(is_new_mode, folder_path):
    video_files = get_file_list(folder_path, VIDEO_EXTENSIONS)
    if not video_files:
        print("No videos found in the selected folder.")
        return
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print("Deleted old CSV file.")

    stt = 1
    to_process = video_files
    print(f"Found {len(to_process)} new videos to process.")

    # === Xử lý song song ===
    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_video, fp, i+stt): fp for i, fp in enumerate(to_process)}
        for i,future in enumerate(as_completed(futures)):
            try:
                result = future.result()
                results.append(result)
                print(f"{i}: {result[1]}")
            except Exception as e:
                print(f" Error processing {futures[future]}: {e}")

    # === Ghi CSV ===
    if results:
        columns = ["stt", "file_path", "duration", "lastest_used_value"]
        new_df = pd.DataFrame(results, columns=columns)

        if not is_new_mode and os.path.exists(OUTPUT_FILE):
            old_df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
            new_df = pd.concat([old_df, new_df], ignore_index=True)

        new_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"\n Done! Saved to {OUTPUT_FILE} ({len(results)} files)")
    else:
        print("No new videos to add.")

def main():
    print("=== Video to CSV (Fast Mode with ffprobe + Multithreading) ===")
    folder_path = input("Enter folder path containing videos: ").strip()
    if not os.path.isdir(folder_path):
        print("Invalid folder path.")
        return

    # Chạy luôn chế độ clear + process all
    run_mode(True, folder_path)

if __name__ == "__main__":
    main()
