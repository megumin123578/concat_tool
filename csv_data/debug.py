import subprocess
import pandas as pd

def get_format_profile(video_path):
    try:
        # Gọi ffprobe để lấy thông tin format profile
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'format=format_name:stream=codec_name,profile',
             '-of', 'default=noprint_wrappers=1:nokey=0',
             video_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return result.stdout.strip()
    except Exception as e:
        return f"Lỗi: {str(e)}"

def main():
    # Đọc file CSV
    df = pd.read_csv("show_asmr_data.csv")  # Thay "data.csv" bằng tên file CSV thật

    # Kiểm tra cột file_path tồn tại không
    if 'file_path' not in df.columns:
        print("Không tìm thấy cột 'file_path' trong file CSV.")
        return

    # Lặp qua từng video path
    for index, row in df.iterrows():
        path = row['file_path']
        print(f"File: {path}")
        profile = get_format_profile(path)
        print(profile)
        print("-" * 50)

if __name__ == "__main__":
    main()
