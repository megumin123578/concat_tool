import os
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
import shutil
import pandas as pd
import numpy as np
import random
from datetime import datetime

# === Cấu hình log ===
LOG_DIR = "log_data\logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")

def log_run(cmd, **kwargs):
    """Chạy subprocess và ghi toàn bộ stdout/stderr vào file log theo ngày."""
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(f"\n=== [{datetime.now().strftime('%H:%M:%S')}] {' '.join(cmd)} ===\n")
        result = subprocess.run(cmd, stdout=log, stderr=log, text=True, **kwargs)
        log.write("\n")
    return result


def load_used_videos(file):
    if os.path.exists(file):
        with open(file, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_used_videos(file, used_set):
    with open(file, 'w', encoding='utf-8') as f:
        for path in used_set:
            f.write(f"{path}\n")


def get_file_name(file_path):
    base_name = os.path.basename(file_path)              
    name_without_ext = os.path.splitext(base_name)[0]    
    return name_without_ext

def get_video_duration(file_path):
    try:
        cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", file_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        duration = float(result.stdout.strip())
        minute = int(duration) // 60
        sec = int(duration) % 60
        return f"{minute}:{sec:02}"
    except Exception as e:
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"[ERROR] get_video_duration({file_path}): {e}\n")
        return "0:00"


def find_first_vid(first_vd):
    first_vd = first_vd.strip().strip('"')
    return first_vd, get_video_duration(first_vd)

def normalize_video(
    input_path,
    output_path,
    width=1920,
    height=1080,
    fps=60,
    use_nvenc=True,
    cq=23,
    v_bitrate="12M",
    a_bitrate="160k",
):
    if not isinstance(input_path, str) or not isinstance(output_path, str):
        raise TypeError(f"Đường dẫn input/output không hợp lệ: input={input_path}, output={output_path}")

    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg không được tìm thấy trong PATH.")

    if use_nvenc and shutil.which("nvidia-smi"):
        vcodec = "h264_nvenc"
        video_args = [
            "-c:v", vcodec,
            "-profile:v", "main",
            "-rc", "vbr",
            "-cq", str(cq),
            "-b:v", v_bitrate,
            "-maxrate", v_bitrate,
            "-bufsize", str(int(int(v_bitrate[:-1]) * 2)) + "M" if v_bitrate.endswith("M") else "16M",
            "-preset", "medium",
            "-vsync", "1",
        ]
    else:
        vcodec = "libx264"
        video_args = [
            "-c:v", vcodec,
            "-preset", "medium",
            "-profile:v", "main",
            "-level", "4.2",
            "-crf", str(cq if isinstance(cq, int) else 20),
            "-maxrate", v_bitrate,
            "-bufsize", "16M",
        ]

    command = [
        "ffmpeg", "-y",
        "-fflags", "+genpts",
        "-i", input_path,
        "-vf", f"scale={width}:{height},fps={fps}",
        *video_args,
        "-pix_fmt", "yuv420p",
        "-r", str(fps),
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-ar", "48000",
        "-b:a", a_bitrate,
        output_path
    ]

    log_run(command, check=True)


def concat_video(video_paths, output_path):
    list_file = "temp.txt"
    with open(list_file, 'w', encoding='utf-8') as f:
        for path in video_paths:
            abs_path = os.path.abspath(path).replace("\\", "/")
            f.write(f"file '{abs_path}'\n")

    command = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", list_file,
        "-c", "copy",
        output_path
    ]
    log_run(command, check=True)
    os.remove(list_file)

def auto_concat(input_videos, output_path):
    normalized_paths = []

    def normalize_and_collect(i, path):
        fixed = f"normalized_{i}.mp4"
        normalize_video(path, fixed)
        return fixed

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(normalize_and_collect, i, path) for i, path in enumerate(input_videos)]
        for future in futures:
            normalized_paths.append(future.result())

    concat_video(normalized_paths, output_path)

    for path in normalized_paths:
        os.remove(path)

    print("Ghép video hoàn tất:", output_path)

# debug
def print_video_info(video_path):
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(f"\n🔍 Đang kiểm tra: {video_path}\n")

    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-print_format", "json",
            "-show_streams", "-show_format",
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        info = json.loads(result.stdout or "{}")

        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(json.dumps(info, indent=2, ensure_ascii=False))
            log.write("\n")
    except Exception as e:
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"Lỗi khi đọc thông tin video: {e}\n")


import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


def update_row_to_sheet(row_index, df_row, sheet_file, worksheet_index):
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    CREDS_FILE = "sheet.json"
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open(sheet_file)

    worksheet = spreadsheet.get_worksheet(worksheet_index)

    gs_row = row_index + 2

    values = df_row.astype(str).fillna('').tolist()

    worksheet.update(f'A{gs_row}', [values])
    print(f"Updated google sheet row {gs_row}")
    




def clear_excel_file(excel_file):
    try:
        columns = ['first vids', 'desired length', 'output directory', 'number_of_vids', 'status']
        empty_df = pd.DataFrame(columns=columns)
        empty_df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Cleared existing content in Excel file: {excel_file}")
    except Exception as e:
        print(f"Error clearing Excel file '{excel_file}': {e}")

def copy_from_ggsheet_to_excel(gspread_client, sheet_name, excel_file,sheet_index):
    try:
        spreadsheet = gspread_client.open(sheet_name)
        worksheet = spreadsheet.get_worksheet(sheet_index)
        data = worksheet.get_all_values()

        if not data:
            print("Google Sheet is empty!")
            return
        
        columns = data[0]  
        values = data[1:]  
        df = pd.DataFrame(values, columns=columns)
        clear_excel_file(excel_file)
        df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Successfully copied data from Google {sheet_name} to Excel file {excel_file}")
    except Exception as e:
        print(f"Error copying data from Google Sheet to Excel: {e}")

def pre_process_data(file):
    df = pd.read_excel(file)
    filtered_df = df[
        df['first vids'].notna() &
        df['desired length'].notna() &
        df['status'].str.lower().eq('auto')
    ]
    return filtered_df, df

def convert_time_to_seconds(time_str):
    try:
        if isinstance(time_str, (int, float)):
            return float(time_str)
        parts = time_str.strip().split(':')
        parts = [int(p) for p in parts]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            return parts[0] * 60 + parts[1]
        elif len(parts) == 1:
            return int(parts[0])
        else:
            return 0
    except:
        return 0
    
def prepare_original_data(csv_file):
    try:
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        durations = np.array([convert_time_to_seconds(d) for d in df['duration']])
        file_paths = df['file_path'].tolist()
        return durations, file_paths, df
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.")
        return None, None, None, None
    except KeyError as e:
        print(f"Error: Missing column {e} in the CSV file.")
        return None, None, None, None
    except Exception as e:
        print(f"Unexpected error reading CSV: {str(e)}")
        return None, None, None, None
    
def generate_video_lists(suitable_df, durations, file_paths, used_video_paths, num_lists=1):

    results = []
    newly_used_paths = set()

    # Duyệt từng dòng trong suitable_df
    for group_index, (row_index, row) in enumerate(suitable_df.iterrows()):
        desired_length = float(row['desired length']) * 60
        first_vid_number = str(row['first vids'])

        # Lấy video đầu
        first_vd = find_first_vid(first_vid_number)
        first_path, first_duration = first_vd[0], convert_time_to_seconds(first_vd[1])
        if not first_path:
            print(f"Không tìm thấy video đầu tiên cho {first_vid_number}")
            continue

        # Lấy second vids nếu có
        second_vid = None
        if 'second vids' in suitable_df.columns:
            sv = row.get('second vids')
            if pd.notna(sv) and str(sv).strip():
                second_vid = str(sv).strip().strip('"')

        # Lấy third vids nếu có
        third_vid = None
        if 'third vids' in suitable_df.columns:
            tv = row.get('third vids')
            if pd.notna(tv) and str(tv).strip():
                third_vid = str(tv).strip().strip('"')

        for list_index in range(num_lists):
            # Chọn các index chưa dùng trong log
            available_indexes = [
                idx for idx in range(len(file_paths))
                if file_paths[idx] not in used_video_paths
            ]

            if not available_indexes:
                print("Đã dùng hết video, reset log.")
                used_video_paths.clear()
                available_indexes = list(range(len(file_paths)))

            total_duration = first_duration
            selected_paths = [first_path]
            newly_used_paths.add(first_path)

            # Thêm second vids
            if second_vid:
                selected_paths.append(second_vid)
                total_duration += convert_time_to_seconds(get_video_duration(second_vid))
                newly_used_paths.add(second_vid)

            # Thêm third vids
            if third_vid:
                selected_paths.append(third_vid)
                total_duration += convert_time_to_seconds(get_video_duration(third_vid))
                newly_used_paths.add(third_vid)

            # Thêm random các video khác cho tới khi đủ desired_length
            while available_indexes and total_duration < desired_length:
                chosen_index = random.choice(available_indexes)
                path = file_paths[chosen_index]

                if path not in used_video_paths:
                    total_duration += durations[chosen_index]
                    selected_paths.append(path)
                    newly_used_paths.add(path)

                available_indexes.remove(chosen_index)

            results.append({
                'name': first_vid_number,
                'group_index': group_index,  # dùng lại trong main để map sang original_df
                'list_number': list_index + 1,
                'selected_files': selected_paths,
                'total_duration': total_duration
            })

    return results, newly_used_paths


def format_and_print_results(results):
    for item in results:
        minutes = int(item['total_duration']) // 60
        seconds = int(item['total_duration']) % 60
        print(f"\nList {item['list_number']}:")
        print(f"Total duration: {minutes:02}:{seconds:02}")
        print("Files:")
        for f in item['selected_files']:
            print("  ", f)
    