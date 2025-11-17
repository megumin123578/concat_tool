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

def excel_to_sheet(excel_file, sheet_file, worksheet_index):
    # Đọc dữ liệu Excel
    df_local = pd.read_excel(excel_file, engine="openpyxl").fillna('')

    # Thiết lập quyền truy cập Google Sheets
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    CREDS_FILE = "sheet.json"
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open(sheet_file)

    try:
        worksheet = spreadsheet.get_worksheet(worksheet_index)
    except Exception as e:
        print(f"Không thể lấy worksheet tại index {worksheet_index}: {e}")
        return

    # Đọc toàn bộ dữ liệu hiện tại từ Google Sheet
    data = worksheet.get_all_values()
    if not data:
        print(" Google Sheet đang rỗng, không thể đối chiếu.")
        return

    # Chuyển sheet thành DataFrame để dễ xử lý
    df_remote = pd.DataFrame(data[1:], columns=data[0]).fillna('')

    # Chuẩn hóa tên cột để khớp với Excel
    df_remote.columns = df_remote.columns.str.strip().str.lower()
    df_local.columns = df_local.columns.str.strip().str.lower()

    if 'first vids' not in df_local.columns:
        print("Không tìm thấy cột 'first vids' trong Excel.")
        return

    # Lấy danh sách các 'first vids' trong Excel (những dòng cần cập nhật)
    target_first_vids = df_local['first vids'].astype(str).tolist()
    updated_count = 0

    # Duyệt từng dòng trong Google Sheet, nếu khớp 'first vids' → update giá trị từ Excel
    for i, row in df_remote.iterrows():
        first_vid_val = str(row.get('first vids', '')).strip()
        if first_vid_val in target_first_vids:
            # Tìm dòng tương ứng trong Excel
            row_local = df_local[df_local['first vids'].astype(str).str.strip() == first_vid_val]
            if not row_local.empty:
                # Lấy hàng đầu tiên khớp
                new_values = row_local.iloc[0].fillna('').astype(str).tolist()
                # Cập nhật toàn bộ dòng (vì có thể nhiều cột thay đổi)
                cell_range = f"A{i+2}"  # +2 vì Google Sheet bắt đầu từ hàng 1 (header)
                worksheet.update(cell_range, [new_values])
                updated_count += 1

    print(f" Đã cập nhật {updated_count} hàng có cùng 'first vids' trong '{sheet_file}'.")



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
    
def generate_video_lists(suitable_df, durations, last_used, file_paths):
    results = []
    for i in range(len(suitable_df)):
        num_lists = 1  # Force number_of_vids to 1
        desired_length = float(suitable_df.iloc[i]['desired length']) * 60
        first_vid_number = str(suitable_df.iloc[i]['first vids'])

        first_vd = find_first_vid(first_vid_number)
        first_path, first_duration = first_vd[0], convert_time_to_seconds(first_vd[1])
        if not first_path:
            print(f"Không tìm thấy video đầu tiên cho {first_vid_number}")
            continue

        for list_index in range(num_lists):
            available_indexes = [
                idx for idx in range(len(file_paths))
                if last_used[idx] >= 0
            ]

            total_duration = first_duration
            selected_indexes = []
            selected_paths = [first_path]

            while available_indexes and total_duration < desired_length:
                chosen_index = random.choice(available_indexes)
                total_duration += durations[chosen_index]
                selected_indexes.append(chosen_index)
                selected_paths.append(file_paths[chosen_index])
                available_indexes.remove(chosen_index)

            results.append({
                'name': first_vid_number,
                'group_index': i,
                'list_number': list_index + 1,
                'selected_files': selected_paths,
                'total_duration': total_duration
            })

    return results

def format_and_print_results(results):
    for item in results:
        minutes = int(item['total_duration']) // 60
        seconds = int(item['total_duration']) % 60
        print(f"\nList {item['list_number']}:")
        print(f"Total duration: {minutes:02}:{seconds:02}")
        print("Files:")
        for f in item['selected_files']:
            print("  ", f)
    