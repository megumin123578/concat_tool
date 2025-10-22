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
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", 
             "-of", "default=noprint_wrappers=1:nokey=1", file_path],
            capture_output=True, text=True
        )
        duration = float(result.stdout.strip())
        minute = int(duration) // 60
        sec = int(duration) % 60
        return f"{minute}:{sec:02}"
    except Exception as e:
        print(f"Error getting duration for '{file_path}': {e}")
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
     # Kiểm tra input/output có đúng định dạng string không
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
            "-preset", "p4",
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
        "-vf", f"scale={width}:{height}:flags=lanczos,fps={fps}",
        *video_args,
        "-pix_fmt", "yuv420p",
        # "-vsync", "cfr",
        "-fps_mode", "cfr",     
        "-r", str(fps),         
        "-movflags", "+faststart",
        "-c:a", "aac",
        "-ar", "48000",
        "-b:a", a_bitrate,
        output_path
    ]

    subprocess.run(command, check=True)



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
    subprocess.run(command, check=True)
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
    print(f"\n🔍 Đang kiểm tra: {video_path}")

    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_format", video_path],
            capture_output=True, text=True, check=True, encoding='utf-8' # THÊM encoding='utf-8'
        )
        info = json.loads(result.stdout)

        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                print(f"VIDEO:")
                print(f"  Codec: {stream.get('codec_name')}")
                print(f"  Resolution: {stream.get('width')}x{stream.get('height')}")
                print(f"  FPS: {eval(stream.get('r_frame_rate')):.2f}")
                print(f"  Pixel format: {stream.get('pix_fmt')}")
            elif stream.get("codec_type") == "audio":
                print(f"AUDIO:")
                print(f"  Codec: {stream.get('codec_name')}")
                print(f"  Sample rate: {stream.get('sample_rate')} Hz")
                print(f"  Channels: {stream.get('channels')}")
        
        format_info = info.get("format", {})
        duration = float(format_info.get("duration", 0))
        print(f"Duration: {duration:.2f} seconds")

    except Exception as e:
        print(f"Lỗi khi đọc thông tin video: {e}")


def excel_to_sheet(excel_file, sheet_file, worksheet_index):
    df = pd.read_excel(excel_file, engine="openpyxl")

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    CREDS_FILE = "sheet.json"

    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open(sheet_file)

    try:
        worksheet = spreadsheet.get_worksheet(worksheet_index)  # Lấy theo index
    except Exception as e:
        print(f"Không thể lấy worksheet tại index {worksheet_index}: {e}")
        return

    worksheet.clear()

    data = [df.columns.tolist()] + df.fillna('').astype(str).values.tolist()
    worksheet.update("A1", data)

    print(f"Đã ghi nội dung vào worksheet index {worksheet_index} trong '{sheet_file}'.")


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
        last_used = np.array([convert_time_to_seconds(t) for t in df['lastest_used_value']])
        file_paths = df['file_path'].tolist()
        return durations, last_used, file_paths, df
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
    